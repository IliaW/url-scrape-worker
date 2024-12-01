package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IliaW/url-scrape-worker/config"
	"github.com/IliaW/url-scrape-worker/internal/aws_s3"
	"github.com/IliaW/url-scrape-worker/internal/broker"
	cacheClient "github.com/IliaW/url-scrape-worker/internal/cache"
	"github.com/IliaW/url-scrape-worker/internal/crawler"
	"github.com/IliaW/url-scrape-worker/internal/model"
	"github.com/IliaW/url-scrape-worker/internal/persistence"
	"github.com/IliaW/url-scrape-worker/internal/worker"
	"github.com/go-sql-driver/mysql"
	"github.com/lmittmann/tint"
)

var (
	cfg          *config.Config
	log          *slog.Logger
	db           *sql.DB
	s3           aws_s3.BucketClient
	cache        cacheClient.CachedClient
	metadataRepo persistence.MetadataStorage
	crawl        *crawler.CommonCrawlerService
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg = config.MustLoad()
	log = setupLogger()
	db = setupDatabase()
	defer closeDatabase()
	s3 = aws_s3.NewS3BucketClient(cfg.S3Settings, log)
	cache = cacheClient.NewMemcachedClient(cfg.CacheSettings, log)
	defer cache.Close()
	crawl = crawler.NewCrawlService(cfg.CrawlerSettings, log)
	metadataRepo = persistence.NewMetadataRepository(db, log)
	scrapeMechanism := model.ScrapeMechanism(cfg.WorkerSetting.ScrapeMechanism)
	log.Info("starting application on port "+cfg.Port, slog.String("env", cfg.Env))

	urlChan := make(chan *model.ScrapeTask, 100) // TODO: Benchmark tests are required to configure the buffer size
	scrapeChan := make(chan *model.Scrape, 100)
	panicChan := make(chan struct{}, cfg.WorkerSetting.MaxWorkers)

	kafkaWg := &sync.WaitGroup{}
	kafkaWg.Add(1)
	go broker.NewKafkaConsumer(ctx, kafkaWg, urlChan, log, cfg.KafkaSettings.Consumer)

	workerWg := &sync.WaitGroup{}
	scrapeWorker := &worker.ScrapeWorker{
		InputChan:       urlChan,
		OutputChan:      scrapeChan,
		PanicChan:       panicChan,
		Crawl:           crawl,
		Cfg:             cfg,
		Log:             log,
		Db:              metadataRepo,
		S3:              s3,
		Cache:           cache,
		Wg:              workerWg,
		ScrapeMechanism: scrapeMechanism,
	}
	for i := 0; i < cfg.WorkerSetting.MaxWorkers; i++ {
		workerWg.Add(1)
		go scrapeWorker.Run()
	}
	// Restart workers if they panic.
	go func() {
		for range panicChan {
			workerWg.Add(1)
			go scrapeWorker.Run()
			time.Sleep(3 * time.Minute) // timeout to avoid polluting logs if something unrecoverable happened
		}
	}()

	kafkaWg.Add(1)
	go broker.NewKafkaProducer(kafkaWg, scrapeChan, log, cfg.KafkaSettings.Producer)

	// Graceful shutdown.
	// 1. Stop Kafka Consumer by system call. Close urlChan
	// 2. Wait till all Workers processed all messages from urlChan. Close scrapeChan
	// 3. Wait till Producer process all messages from scrapeChan and write to kafka
	// 4. Stop Kafka Producer. Close database and memcached connections
	<-ctx.Done()
	log.Info("stopping server...")
	workerWg.Wait()
	close(scrapeChan)
	log.Info("close scrapeChan.")
	close(panicChan)
	log.Info("close panicChan.")
	kafkaWg.Wait()
}

func setupLogger() *slog.Logger {
	resolvedLogLevel := func() slog.Level {
		envLogLevel := strings.ToLower(cfg.LogLevel)
		switch envLogLevel {
		case "info":
			return slog.LevelInfo
		case "error":
			return slog.LevelError
		default:
			return slog.LevelDebug
		}
	}

	replaceAttrs := func(groups []string, a slog.Attr) slog.Attr {
		if a.Key == slog.SourceKey {
			source := a.Value.Any().(*slog.Source)
			source.File = filepath.Base(source.File)
		}
		return a
	}

	var logger *slog.Logger
	if strings.ToLower(cfg.LogType) == "json" {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			AddSource:   true,
			Level:       resolvedLogLevel(),
			ReplaceAttr: replaceAttrs}))
	} else {
		logger = slog.New(tint.NewHandler(os.Stdout, &tint.Options{
			AddSource:   true,
			Level:       resolvedLogLevel(),
			ReplaceAttr: replaceAttrs,
			NoColor:     false}))
	}

	slog.SetDefault(logger)
	logger.Debug("debug messages are enabled.")

	return logger
}

func setupDatabase() *sql.DB {
	log.Info("connecting to the database...")
	sqlCfg := mysql.Config{
		User:                 cfg.DbSettings.User,
		Passwd:               cfg.DbSettings.Password,
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("%s:%s", cfg.DbSettings.Host, cfg.DbSettings.Port),
		DBName:               cfg.DbSettings.Name,
		AllowNativePasswords: true,
		ParseTime:            true,
	}
	database, err := sql.Open("mysql", sqlCfg.FormatDSN())
	if err != nil {
		log.Error("failed to establish database connection.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	database.SetConnMaxLifetime(cfg.DbSettings.ConnMaxLifetime)
	database.SetMaxOpenConns(cfg.DbSettings.MaxOpenConns)
	database.SetMaxIdleConns(cfg.DbSettings.MaxIdleConns)

	maxRetry := 6
	for i := 1; i <= maxRetry; i++ {
		log.Info("ping the database.", slog.String("attempt", fmt.Sprintf("%d/%d", i, maxRetry)))
		pingErr := database.Ping()
		if pingErr != nil {
			log.Error("not responding.", slog.String("err", pingErr.Error()))
			if i == maxRetry {
				log.Error("failed to establish database connection.")
				os.Exit(1)
			}
			log.Info(fmt.Sprintf("wait %d seconds", 5*i))
			time.Sleep(time.Duration(5*i) * time.Second)
		} else {
			break
		}
	}
	log.Info("connected to the database!")

	return database
}

func closeDatabase() {
	log.Info("closing database connection.")
	err := db.Close()
	if err != nil {
		log.Error("failed to close database connection.", slog.String("err", err.Error()))
	}
}
