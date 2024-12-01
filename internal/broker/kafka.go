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

// NewKafkaProducer takes scraped pages from scrapeChan and sends them to kafka.
// After shutdown, the function will continue execution until the channel scrapeChan runs out of scraps
func NewKafkaProducer(wg *sync.WaitGroup, scrapeChan <-chan *model.Scrape, log *slog.Logger,
	cfg *config.ProducerConfig) {
	defer wg.Done()
	log.Info("starting kafka producer...", slog.String("topic", cfg.WriteTopicName))

	w := kafka.Writer{
		Addr:         kafka.TCP(strings.Split(cfg.Addr, ",")...),
		Topic:        cfg.WriteTopicName,
		Balancer:     &kafka.Hash{},
		MaxAttempts:  cfg.MaxAttempts,
		BatchSize:    1,                // the parameter is controlled by 'batchTicker' variable
		BatchTimeout: time.Millisecond, // the parameter is controlled by 'batch' variable
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		RequiredAcks: kafka.RequiredAcks(cfg.RequiredAsks),
		Async:        cfg.Async,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				log.Error("failed to send messages to kafka.", slog.String("err", err.Error()))
			}
		},
		Compression: kafka.Compression(new(lz4.Codec).Code()),
	}
	defer func() {
		err := w.Close()
		if err != nil {
			log.Error("failed to close kafka writer.", slog.String("err", err.Error()))
		}
	}()

	batchTicker := time.NewTicker(cfg.BatchTimeout)
	batch := make([]kafka.Message, 0, cfg.BatchSize)
	writeMessage := func(batch []kafka.Message) {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.WriteTimeout)
		defer cancel()
		err := w.WriteMessages(ctx, batch...)
		if err != nil {
			log.Error("failed to send messages to kafka.", slog.String("err", err.Error()))
			return
		}
		log.Debug("successfully sent messages to kafka.", slog.Int("batch length", len(batch)))
	}

	for scrape := range scrapeChan {
		body, err := json.Marshal(scrape)
		if err != nil {
			log.Error("marshaling error.", slog.String("err", err.Error()), slog.Any("scrape", scrape))
			continue
		}
		batch = append(batch, kafka.Message{
			Key:   []byte(scrape.FullURL),
			Value: body,
		})
		select {
		case <-batchTicker.C:
			writeMessage(batch)
			batch = make([]kafka.Message, 0, cfg.BatchSize)
		default:
			if len(batch) >= cfg.BatchSize {
				writeMessage(batch)
				batch = make([]kafka.Message, 0, cfg.BatchSize)
			}
		}
	}
	// Some messages may remain in the batch after scrapeChan is closed
	if len(batch) > 0 {
		log.Debug("messages in batch.", slog.Int("count", len(batch)))
		writeMessage(batch)
	}
	log.Info("stopping kafka writer.")
}

// NewKafkaConsumer reads messages (URLs  for scraping) from kafka and sends them to the urlChan.
// The function close the urlChan and reader when the context is done.
func NewKafkaConsumer(ctx context.Context, wg *sync.WaitGroup, urlChan chan<- *model.ScrapeTask, log *slog.Logger,
	cfg *config.ConsumerConfig) {
	log.Info("starting kafka consumer.", slog.String("topic", cfg.ReadTopicName))
	defer wg.Done()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:          strings.Split(cfg.Brokers, ","),
		Topic:            cfg.ReadTopicName,
		GroupID:          cfg.GroupID,
		MaxWait:          cfg.MaxWait,
		ReadBatchTimeout: cfg.ReadBatchTimeout,
	})

	for {
		select {
		case <-ctx.Done():
			log.Info("stopping kafka reader.")
			err := r.Close()
			if err != nil {
				log.Error("failed to close kafka reader.", slog.String("err", err.Error()))
			}
			close(urlChan)
			log.Info("close urlChan.")
			return
		default:
			m, err := r.ReadMessage(ctx)
			if err != nil {
				log.Error("failed to read message from kafka.", slog.String("err", err.Error()))
				continue
			}
			log.Debug("successfully read messages from kafka.")

			var task model.ScrapeTask
			if err = json.Unmarshal(m.Value, &task); err != nil {
				log.Error("failed to unmarshal message.", slog.String("err", err.Error()))
				continue
			}
			urlChan <- &task
		}
	}
}
