package model

type ScrapeMechanism int

const (
	Curl ScrapeMechanism = iota
	HeadlessBrowser
)

func (sm ScrapeMechanism) String() string {
	return [...]string{"curl", "headless browser"}[sm]
}

type Scrape struct {
	Title               string `json:"title"`
	FullURL             string `json:"full_url"`
	FullHTML            string `json:"full_html,omitempty"`
	TimeToScrape        int64  `json:"time_to_scrape"` // in milliseconds
	StatusCode          int    `json:"status_code"`
	Status              string `json:"status"`
	ScrapeMechanism     string `json:"scrape_mechanism"`
	ScrapeWorkerVersion string `json:"scrape_worker_version"`
	ETag                string `json:"etag,omitempty"`
}

type ScrapeTask struct {
	URL               string `json:"url"`
	IsAllowedToScrape bool   `json:"allowed_to_scrape"`
}
