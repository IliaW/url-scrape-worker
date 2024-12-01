package persistence

import (
	"database/sql"
	"log/slog"

	"github.com/IliaW/url-scrape-worker/internal/model"
)

type MetadataStorage interface {
	Save(*model.Scrape)
}

type MetadataRepository struct {
	db  *sql.DB
	log *slog.Logger
}

func NewMetadataRepository(db *sql.DB, log *slog.Logger) *MetadataRepository {
	return &MetadataRepository{db: db, log: log}
}

func (mr *MetadataRepository) Save(scrape *model.Scrape) {
	_, err := mr.db.Exec("INSERT INTO scrape_metadata (url, time_to_scrape, status, status_code, scrape_mechanism, scrape_worker_version, e_tag) VALUES (?, ?, ?, ?, ?, ?, ?)",
		//scrape.Title,
		scrape.FullURL,
		//scrape.FullHTML,
		scrape.TimeToScrape,
		scrape.Status,
		scrape.StatusCode,
		scrape.ScrapeMechanism,
		scrape.ScrapeWorkerVersion,
		scrape.ETag)
	if err != nil {
		mr.log.Error("failed to save scrape metadata to database.", slog.String("err", err.Error()))
		return
	}
	mr.log.Debug("scrape metadata saved to db.")
}
