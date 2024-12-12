package worker

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/IliaW/url-scrape-worker/config"
	"github.com/IliaW/url-scrape-worker/internal/aws_s3"
	"github.com/IliaW/url-scrape-worker/internal/cache"
	"github.com/IliaW/url-scrape-worker/internal/crawler"
	"github.com/IliaW/url-scrape-worker/internal/model"
	"github.com/IliaW/url-scrape-worker/internal/persistence"
	"github.com/chromedp/cdproto/dom"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
	"github.com/gocolly/colly"
)

type ScrapeWorker struct {
	InputChan       <-chan *model.ScrapeTask
	OutputChan      chan<- *model.Scrape
	PanicChan       chan struct{}
	Crawl           *crawler.CommonCrawlerService
	Cfg             *config.Config
	Log             *slog.Logger
	Db              persistence.MetadataStorage
	S3              aws_s3.BucketClient
	Cache           cache.CachedClient
	Wg              *sync.WaitGroup
	ScrapeMechanism model.ScrapeMechanism
}

// Run starts the scrape worker. It will scrape the url and send the result to the output channel.
func (w *ScrapeWorker) Run() {
	defer func() {
		if r := recover(); r != nil {
			w.Log.Error("PANIC!", slog.Any("err", r))
			w.PanicChan <- struct{}{}
		}
	}()
	defer w.Wg.Done()
	w.Log.Debug("starting scrape worker.")

	for task := range w.InputChan {
		scrape := &model.Scrape{
			FullURL:             task.URL,
			ScrapeWorkerVersion: w.Cfg.Version,
		}
		err := error(nil)
		if task.IsAllowedToScrape {
			scrape, err = w.scrapeUrl(scrape)
			if err != nil {
				w.Log.Error("scraping failed.", slog.String("err", err.Error()))
				continue
				// TODO: action for failed scrapes?
				// 1. Kafka topic for failed scrapes,
				// 2. Use CommonCrawl or
				// 3. Skip?
			}
			// Retries with exponential backoff for 429 status code
			for retry, delay := w.Cfg.WorkerSettings.RetryAttempts, w.Cfg.WorkerSettings.RetryDelay; scrape.StatusCode ==
				http.StatusTooManyRequests && retry > 0; retry, delay = retry-1, delay*2 {
				w.Log.Warn("too many requests status code. retrying...", slog.Int("attempts left", retry))
				time.Sleep(delay)
				scrape, err = w.scrapeUrl(scrape)
				if err != nil {
					w.Log.Error("scraping failed.", slog.String("err", err.Error()))
					continue
				}
			}
			if scrape.StatusCode == http.StatusTooManyRequests {
				w.Log.Error("to many requests error. Scraping failed.", slog.String("task", task.URL))
				continue
			}
		} else {
			scrape, err = w.Crawl.GetScrape(scrape)
			if err != nil {
				w.Log.Error("scraping failed.", slog.String("err", err.Error()))
				continue
			}
		}
		w.saveScrape(scrape)
	}
}

// TODO: should we stop saving and sending a scrape to kafka if writing to S3 will fail?
func (w *ScrapeWorker) saveScrape(scrape *model.Scrape) {
	w.Cache.DecrementThreshold(scrape.FullURL) // Decrease threshold for the url in Cache
	link := w.S3.WriteScrape(scrape)           // Save to S3
	w.Cache.SaveS3Link(scrape.FullURL, link)   // Save the S3 link to Cache
	w.Db.Save(scrape)                          // Save metadata to database
	w.OutputChan <- scrape                     // Send the scrape to kafka producer
}

func (w *ScrapeWorker) scrapeUrl(s *model.Scrape) (*model.Scrape, error) {
	switch w.ScrapeMechanism {
	case model.Curl:
		return w.scrapeWithCurl(s)
	case model.HeadlessBrowser:
		return w.scrapeWithBrowser(s)
	default:
		return nil, errors.New("unsupported scrape mechanism")
	}
}

// scrapeWithCurl copied from the Umbrella project for compatibility
func (w *ScrapeWorker) scrapeWithCurl(scrape *model.Scrape) (*model.Scrape, error) {
	scrape.ScrapeMechanism = w.ScrapeMechanism.String()

	c := colly.NewCollector()
	c.SetRequestTimeout(w.Cfg.WorkerSettings.ScrapeTimeout)
	c.UserAgent = w.Cfg.WorkerSettings.UserAgent

	c.OnResponse(func(resp *colly.Response) {
		scrape.FullHTML = string(resp.Body)
		scrape.ETag = resp.Headers.Get("ETag")
	})
	c.OnHTML("title", func(e *colly.HTMLElement) {
		scrape.Title = e.Text
	})

	c.OnError(func(r *colly.Response, err error) {
		scrape.StatusCode = -1
		if len(err.Error()) > 1000 {
			scrape.Status = err.Error()[:1000]
		} else {
			scrape.Status = err.Error()
		}
	})

	if !strings.HasPrefix(scrape.FullURL, "http://") && !strings.HasPrefix(scrape.FullURL, "https://") {
		scrape.FullURL = "https://" + scrape.FullURL
	}

	t := time.Now()
	err := c.Visit(scrape.FullURL)
	scrape.TimeToScrape = time.Since(t).Milliseconds()
	if err != nil {
		// error / status are captured by the callback above
		return scrape, nil
	}
	scrape.StatusCode = 200
	scrape.Status = http.StatusText(200)

	return scrape, nil
}

func (w *ScrapeWorker) scrapeWithBrowser(scrape *model.Scrape) (*model.Scrape, error) {
	startTime := time.Now()
	responseHeaders := make(map[string]interface{}, 20)
	scrape.ScrapeMechanism = w.ScrapeMechanism.String()

	tCtx, cancelTCtx := context.WithTimeout(context.Background(), w.Cfg.WorkerSettings.ScrapeTimeout)
	defer cancelTCtx()
	ctx, cancel := chromedp.NewContext(tCtx)
	defer cancel()

	chromedp.ListenTarget(ctx, func(event interface{}) {
		switch responseReceivedEvent := event.(type) {
		case *network.EventResponseReceived:
			response := responseReceivedEvent.Response
			if response.URL == scrape.FullURL {
				scrape.StatusCode = int(response.Status)
				if len(response.StatusText) > 1000 {
					scrape.Status = response.StatusText[:1000]
				} else {
					scrape.Status = response.StatusText
				}
				responseHeaders = response.Headers
			}
		case *network.EventRequestWillBeSent:
			request := responseReceivedEvent.Request
			if responseReceivedEvent.RedirectResponse != nil {
				scrape.FullURL = request.URL
				w.Log.Info("redirected.", slog.String("url",
					responseReceivedEvent.RedirectResponse.URL))
			}
		}
	})
	err := chromedp.Run(ctx,
		chromedp.Tasks{
			network.Enable(),
			network.SetExtraHTTPHeaders(map[string]interface{}{
				"User-Agent": w.Cfg.WorkerSettings.UserAgent,
			}),
			enableLifeCycleEvents(),
			navigateAndWaitFor(scrape.FullURL, "networkIdle"),
		},
		chromedp.Title(&scrape.Title),
		chromedp.ActionFunc(func(ctx context.Context) error {
			rootNode, err := dom.GetDocument().Do(ctx)
			if err != nil {
				return err
			}
			scrape.FullHTML, err = dom.GetOuterHTML().WithNodeID(rootNode.NodeID).Do(ctx)
			return err
		}),
	)
	if responseHeaders["ETag"] != nil {
		scrape.ETag = responseHeaders["ETag"].(string)
	}
	scrape.TimeToScrape = time.Since(startTime).Milliseconds()

	return scrape, err
}

func enableLifeCycleEvents() chromedp.ActionFunc {
	return func(ctx context.Context) error {
		err := page.Enable().Do(ctx)
		if err != nil {
			return err
		}
		err = page.SetLifecycleEventsEnabled(true).Do(ctx)
		if err != nil {
			return err
		}
		return nil
	}
}

func navigateAndWaitFor(url string, eventName string) chromedp.ActionFunc {
	return func(ctx context.Context) error {
		_, _, _, err := page.Navigate(url).Do(ctx)
		if err != nil {
			return err
		}
		return waitFor(ctx, eventName)
	}
}

func waitFor(ctx context.Context, eventName string) error {
	ch := make(chan struct{})
	cctx, cancel := context.WithCancel(ctx)
	chromedp.ListenTarget(cctx, func(ev interface{}) {
		switch e := ev.(type) {
		case *page.EventLifecycleEvent:
			if e.Name == eventName {
				cancel()
				close(ch)
			}
		}
	})
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
