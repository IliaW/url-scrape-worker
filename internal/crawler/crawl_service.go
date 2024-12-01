package crawler

import (
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/IliaW/url-scrape-worker/config"
	"github.com/IliaW/url-scrape-worker/internal/model"
	jsoniter "github.com/json-iterator/go"
	"github.com/karust/gogetcrawl/common"
	"github.com/karust/gogetcrawl/commoncrawl"
	"github.com/patrickmn/go-cache"
)

const indexListUrl = "https://index.commoncrawl.org/collinfo.json"

type Index struct {
	Id       string `json:"id"`
	Name     string `json:"name"`
	Timegate string `json:"timegate"`
	CdxAPI   string `json:"cdx-api"`
}

type CommonCrawlerService struct {
	crawler    *commoncrawl.CommonCrawl
	cfg        *config.CrawlerConfig
	log        *slog.Logger
	localCache *cache.Cache
}

func NewCrawlService(cfg *config.CrawlerConfig, log *slog.Logger) *CommonCrawlerService {
	c, err := commoncrawl.New(cfg.RequestTimeout, cfg.Retries)
	if err != nil {
		log.Error("failed to create common crawl client", slog.String("err", err.Error()))
	}
	return &CommonCrawlerService{
		crawler:    c,
		cfg:        cfg,
		log:        log,
		localCache: cache.New(72*time.Hour, 72*time.Hour), // indexes update every month
	}
}

func (c *CommonCrawlerService) GetScrape(scrape *model.Scrape) (*model.Scrape, error) {
	startTime := time.Now()
	if c.crawler == nil { // due to request limitations, the crawler may not be initialized when the application starts
		c.log.Info("connection retry to common crawl.")
		var err error
		c.crawler, err = commoncrawl.New(c.cfg.RequestTimeout, c.cfg.Retries)
		if err != nil {
			c.log.Error("failed to create common crawl client", slog.String("err", err.Error()))
			return scrape, errors.New("connection to common crawl failed")
		}
	}
	scrape.ScrapeMechanism = c.crawler.Name()

	indexList, err := c.getIndexes()
	if err != nil {
		return scrape, err
	}
	requestCfg := common.RequestConfig{
		URL:     scrape.FullURL,
		Filters: []string{"statuscode:200", "mimetype:text/html"},
	}

	for i := 0; i < c.cfg.LastCrawlIndexes; i++ {
		p, _ := c.crawler.GetPagesIndex(requestCfg, indexList[i].Id)
		if len(p) == 0 {
			c.log.Debug("no scrapes found", slog.String("url", scrape.FullURL),
				slog.String("index", indexList[i].Id))
			continue
		}
		resp, err := c.crawler.GetFile(p[len(p)-1]) // last one is the most recent
		if err != nil {
			c.log.Error("failed to get file", slog.String("err", err.Error()))
			break
		}
		body := string(resp)
		scrape.Title = extractTitle(&body)
		scrape.FullHTML = extractHtml(&body)
		scrape.StatusCode = http.StatusOK
		scrape.Status = http.StatusText(http.StatusOK)
		scrape.ETag = extractEtag(&body)
		break
	}
	if scrape.FullHTML == "" {
		c.log.Info("no scrapes found", slog.String("url", scrape.FullURL))
		return scrape, errors.New("no scrapes found")
	}
	scrape.TimeToScrape = time.Since(startTime).Milliseconds()

	return scrape, nil
}

func (c *CommonCrawlerService) getIndexes() ([]Index, error) {
	if i, ok := c.localCache.Get("indexes"); ok {
		return i.([]Index), nil
	}

	response, err := common.Get(indexListUrl, c.crawler.MaxTimeout, c.crawler.MaxRetries)
	if err != nil {
		return nil, err
	}

	var indexes []Index
	err = jsoniter.Unmarshal(response, &indexes)
	if err != nil {
		return indexes, err
	}
	c.localCache.Set("indexes", indexes, cache.DefaultExpiration)

	return indexes, nil
}

func extractEtag(body *string) string {
	r := regexp.MustCompile(`(?i)Etag:\s*"([^"]+)"`)
	match := r.FindStringSubmatch(*body)

	if len(match) > 1 {
		return match[1]
	}
	return ""
}

func extractTitle(body *string) string {
	re := regexp.MustCompile(`<title>(.*?)</title>`)
	match := re.FindStringSubmatch(*body)

	if len(match) > 1 {
		return match[1]
	}
	return ""
}

func extractHtml(body *string) string {
	re := regexp.MustCompile(`(?si)<!doctype html>.*?</html>`)
	match := re.FindStringSubmatch(*body)

	if len(match) > 0 {
		return match[0]
	}
	return ""
}
