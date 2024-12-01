package aws_s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/IliaW/url-scrape-worker/config"
	"github.com/IliaW/url-scrape-worker/internal/model"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	crd "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type BucketClient interface {
	WriteScrape(*model.Scrape) string
}

type S3BucketClient struct {
	client *s3.Client
	cfg    *config.S3Config
	log    *slog.Logger
}

func NewS3BucketClient(cfg *config.S3Config, log *slog.Logger) *S3BucketClient {
	log.Info("connecting to s3...")
	ctx := context.Background()

	sqsConfig, err := awsCfg.LoadDefaultConfig(ctx,
		awsCfg.WithCredentialsProvider(crd.NewStaticCredentialsProvider(cfg.AwsAccessKey, cfg.AwsSecretKey, "")),
		awsCfg.WithRegion(cfg.Region),
		awsCfg.WithBaseEndpoint(cfg.AwsBaseEndpoint))
	if err != nil {
		log.Error("failed to load s3 config.", slog.String("err", err.Error()))
		os.Exit(1)
	}

	// LocalStack does not support `virtual host addressing style` that uses s3 by default.
	// For test purposes use configuration with disabled 'virtual hosted bucket addressing'.
	var s3client *s3.Client
	if cfg.AwsAccessKey == "test" {
		log.Warn("test configuration for s3")
		s3client = s3.NewFromConfig(sqsConfig, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	} else {
		s3client = s3.NewFromConfig(sqsConfig)
	}
	log.Info("connected to s3")

	return &S3BucketClient{
		client: s3client,
		cfg:    cfg,
		log:    log,
	}
}

func (bc *S3BucketClient) WriteScrape(scrape *model.Scrape) string {
	hash := sha256.New()
	hash.Write([]byte(scrape.FullURL))
	hashUrl := hex.EncodeToString(hash.Sum(nil))

	s3Key := fmt.Sprintf("%s/%s/%s", bc.cfg.KeyPrefix, hashUrl, "scrape.json")
	body, err := json.Marshal(scrape)
	if err != nil {
		bc.log.Error("marshaling failed.", slog.String("err", err.Error()))
		return ""
	}

	_, err = bc.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: &bc.cfg.BucketName,
		Key:    &s3Key,
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		bc.log.Error("failed to save scrape to s3.", slog.String("err", err.Error()))
		return ""
	}
	bc.log.Debug("scrape saved to s3.")

	return fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", bc.cfg.BucketName, bc.cfg.Region, s3Key)
}
