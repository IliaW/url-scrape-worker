package broker

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/IliaW/url-scrape-worker/config"
	"github.com/IliaW/url-scrape-worker/internal/model"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress/lz4"
)

type KafkaProducerClient struct {
	scrapeChan <-chan *model.Scrape
	cfg        *config.ProducerConfig
	log        *slog.Logger
	wg         *sync.WaitGroup
}

func NewKafkaProducer(scrapeChan <-chan *model.Scrape, cfg *config.ProducerConfig, log *slog.Logger,
	wg *sync.WaitGroup) *KafkaProducerClient {
	return &KafkaProducerClient{
		scrapeChan: scrapeChan,
		cfg:        cfg,
		log:        log,
		wg:         wg,
	}
}

func (p *KafkaProducerClient) Run() {
	defer p.wg.Done()
	p.log.Info("starting kafka producer...", slog.String("topic", p.cfg.WriteTopicName))

	w := kafka.Writer{
		Addr:         kafka.TCP(strings.Split(p.cfg.Addr, ",")...),
		Topic:        p.cfg.WriteTopicName,
		Balancer:     &kafka.Hash{},
		MaxAttempts:  p.cfg.MaxAttempts,
		BatchSize:    1,                // the parameter is controlled by 'batchTicker' variable
		BatchTimeout: time.Millisecond, // the parameter is controlled by 'batch' variable
		ReadTimeout:  p.cfg.ReadTimeout,
		WriteTimeout: p.cfg.WriteTimeout,
		RequiredAcks: kafka.RequiredAcks(p.cfg.RequiredAsks),
		Async:        p.cfg.Async,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				p.log.Error("failed to send messages to kafka.", slog.String("err", err.Error()))
			}
		},
		Compression: kafka.Compression(new(lz4.Codec).Code()),
	}
	defer func() {
		err := w.Close()
		if err != nil {
			p.log.Error("failed to close kafka writer.", slog.String("err", err.Error()))
		}
	}()

	batchTicker := time.NewTicker(p.cfg.BatchTimeout)
	batch := make([]kafka.Message, 0, p.cfg.BatchSize)
	writeMessage := func(batch []kafka.Message) {
		ctx, cancel := context.WithTimeout(context.Background(), p.cfg.WriteTimeout)
		defer cancel()
		err := w.WriteMessages(ctx, batch...)
		if err != nil {
			p.log.Error("failed to send messages to kafka.", slog.String("err", err.Error()))
			return
		}
		p.log.Debug("successfully sent messages to kafka.", slog.Int("batch length", len(batch)))
	}

	for scrape := range p.scrapeChan {
		body, err := json.Marshal(scrape)
		if err != nil {
			p.log.Error("marshaling error.", slog.String("err", err.Error()), slog.Any("scrape", scrape))
			continue
		}
		batch = append(batch, kafka.Message{
			Key:   []byte(scrape.FullURL),
			Value: body,
		})
		select {
		case <-batchTicker.C:
			writeMessage(batch)
			batch = batch[:0]
		default:
			if len(batch) >= p.cfg.BatchSize {
				writeMessage(batch)
				batch = batch[:0]
			}
		}
	}
	// Some messages may remain in the batch after scrapeChan is closed
	if len(batch) > 0 {
		p.log.Debug("messages in batch.", slog.Int("count", len(batch)))
		writeMessage(batch)
	}
	p.log.Info("stopping kafka writer.")
}

type KafkaConsumerClient struct {
	urlChan chan<- *model.ScrapeTask
	cfg     *config.ConsumerConfig
	log     *slog.Logger
	wg      *sync.WaitGroup
}

func NewKafkaConsumer(urlChan chan<- *model.ScrapeTask, cfg *config.ConsumerConfig, log *slog.Logger,
	wg *sync.WaitGroup) *KafkaConsumerClient {
	return &KafkaConsumerClient{
		urlChan: urlChan,
		cfg:     cfg,
		log:     log,
		wg:      wg,
	}
}

func (c *KafkaConsumerClient) Run(ctx context.Context) {
	c.log.Info("starting kafka consumer.", slog.String("topic", c.cfg.ReadTopicName))
	defer c.wg.Done()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:          strings.Split(c.cfg.Brokers, ","),
		Topic:            c.cfg.ReadTopicName,
		GroupID:          c.cfg.GroupID,
		MaxWait:          c.cfg.MaxWait,
		ReadBatchTimeout: c.cfg.ReadBatchTimeout,
	})

	for {
		select {
		case <-ctx.Done():
			c.log.Info("stopping kafka reader.")
			err := r.Close()
			if err != nil {
				c.log.Error("failed to close kafka reader.", slog.String("err", err.Error()))
			}
			close(c.urlChan)
			c.log.Info("close urlChan.")
			return
		default:
			m, err := r.ReadMessage(ctx)
			if err != nil {
				c.log.Error("failed to read message from kafka.", slog.String("err", err.Error()))
				continue
			}
			c.log.Debug("successfully read messages from kafka.")

			var task model.ScrapeTask
			if err = json.Unmarshal(m.Value, &task); err != nil {
				c.log.Error("failed to unmarshal message.", slog.String("err", err.Error()))
				continue
			}
			c.urlChan <- &task
		}
	}
}
