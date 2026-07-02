package pget

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"golang.org/x/sync/errgroup"
)

type assignTasksConfig struct {
	Procs         int
	TaskSize      int64 // download filesize per task
	ContentLength int64 // full download filesize
	URLs          []string
	PartialDir    string
	Filename      string
	Client        *http.Client
}

type task struct {
	ID         int
	Procs      int
	URL        string
	Range      Range
	PartialDir string
	Filename   string
	Client     *http.Client
}

func (t *task) destPath() string {
	return getPartialFilePath(t.PartialDir, t.Filename, t.Procs, t.ID)
}

func (t *task) String() string {
	return fmt.Sprintf("task[%d]: %q", t.ID, t.destPath())
}

func (t *task) remainingBytes() int64 {
	if t.ID == t.Procs-1 {
		return t.Range.high - t.Range.low
	}
	return t.Range.high - t.Range.low + 1
}

type makeRequestOption struct {
	useragent string
	referer   string
}

func (t *task) makeRequest(ctx context.Context, opt *makeRequestOption) (*http.Request, error) {
	req, err := http.NewRequest("GET", t.URL, nil)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to make a new request: %d", t.ID))
	}
	req = req.WithContext(ctx)

	// set download ranges
	req.Header.Set("Range", t.Range.BytesRange())

	// set useragent
	req.Header.Set("User-Agent", opt.useragent)

	// set referer
	if opt.referer != "" {
		req.Header.Set("Referer", opt.referer)
	}

	return req, nil
}

// assignTasks creates task to assign it to each goroutines
func assignTasks(c *assignTasksConfig) []*task {
	tasks := make([]*task, 0, c.Procs)

	var totalActiveProcs int
	for i := 0; i < c.Procs; i++ {

		r := makeRange(i, c.Procs, c.TaskSize, c.ContentLength)

		partName := getPartialFilePath(c.PartialDir, c.Filename, c.Procs, i)

		if info, err := os.Stat(partName); err == nil {
			infosize := info.Size()
			// check if the part is fully downloaded
			if i == c.Procs-1 {
				if infosize == r.high-r.low {
					continue
				}
			} else if infosize == c.TaskSize {
				// skip as the part is already downloaded
				continue
			}

			// make low range from this next byte
			r.low += infosize
		}

		tasks = append(tasks, &task{
			ID:         i,
			Procs:      c.Procs,
			URL:        c.URLs[totalActiveProcs%len(c.URLs)],
			Range:      r,
			PartialDir: c.PartialDir,
			Filename:   c.Filename,
			Client:     c.Client,
		})

		totalActiveProcs++
	}

	return tasks
}

type DownloadConfig struct {
	Filename      string
	Dirname       string
	ContentLength int64
	Procs         int
	URLs          []string
	Client        *http.Client

	*makeRequestOption
}

type DownloadOption func(c *DownloadConfig)

func WithUserAgent(ua, version string) DownloadOption {
	return func(c *DownloadConfig) {
		c.makeRequestOption.useragent = resolveUserAgent(ua)
	}
}

func WithReferer(referer string) DownloadOption {
	return func(c *DownloadConfig) {
		c.makeRequestOption.referer = referer
	}
}

func Download(ctx context.Context, c *DownloadConfig, opts ...DownloadOption) error {
	partialDir := getPartialDirname(c.Dirname, c.Filename, c.Procs)

	// create download location
	if err := os.MkdirAll(partialDir, 0755); err != nil {
		return errors.Wrap(err, "failed to mkdir for download location")
	}

	c.makeRequestOption = &makeRequestOption{}

	for _, opt := range opts {
		opt(c)
	}

	tasks := assignTasks(&assignTasksConfig{
		Procs:         c.Procs,
		TaskSize:      c.ContentLength / int64(c.Procs),
		ContentLength: c.ContentLength,
		URLs:          c.URLs,
		PartialDir:    partialDir,
		Filename:      c.Filename,
		Client:        newClient(c.Client),
	})

	if err := parallelDownload(ctx, &parallelDownloadConfig{
		ContentLength:     c.ContentLength,
		Tasks:             tasks,
		PartialDir:        partialDir,
		makeRequestOption: c.makeRequestOption,
	}); err != nil {
		return err
	}

	return bindFiles(c, partialDir)
}

type parallelDownloadConfig struct {
	ContentLength int64
	Tasks         []*task
	PartialDir    string
	*makeRequestOption
}

func newProgressContainer() *mpb.Progress {
	return mpb.New(
		mpb.WithOutput(stdout),
		mpb.WithWidth(64),
	)
}

func barDecorators(name string) []mpb.BarOption {
	return []mpb.BarOption{
		mpb.BarFillerOnComplete("="),
		mpb.PrependDecorators(
			decor.Name(name, decor.WC{C: decor.DSyncWidth | decor.DextraSpace}),
		),
		mpb.AppendDecorators(
			decor.Counters(decor.SizeB1024(0), "%.1f / %.1f"),
			decor.Percentage(decor.WC{W: 5}),
			decor.AverageSpeed(decor.SizeB1024(0), "% .1f"),
			decor.OnComplete(
				decor.AverageETA(decor.ET_STYLE_GO, decor.WC{W: 8}),
				"done",
			),
		),
	}
}

func parallelDownload(ctx context.Context, c *parallelDownloadConfig) error {
	eg, ctx := errgroup.WithContext(ctx)

	p := newProgressContainer()

	downloaded, err := checkProgress(c.PartialDir)
	if err != nil {
		return errors.Wrap(err, "failed to get directory size")
	}

	totalBar := p.AddBar(c.ContentLength, barDecorators("Total")...)
	totalBar.SetCurrent(downloaded)

	taskBars := make(map[int]*mpb.Bar, len(c.Tasks))
	for _, task := range c.Tasks {
		taskBars[task.ID] = p.AddBar(task.remainingBytes(), barDecorators(fmt.Sprintf("#%d", task.ID))...)
	}

	for _, task := range c.Tasks {
		task := task
		bar := taskBars[task.ID]
		eg.Go(func() error {
			req, err := task.makeRequest(ctx, c.makeRequestOption)
			if err != nil {
				return err
			}
			if err := task.download(req, bar, totalBar); err != nil {
				return err
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		p.Shutdown()
		return err
	}

	p.Wait()
	return nil
}

func (t *task) download(req *http.Request, bar, totalBar *mpb.Bar) error {
	resp, err := t.Client.Do(req)
	if err != nil {
		return errors.Wrapf(err, "failed to get response: %q", t.String())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return errors.Errorf("unexpected status %d for range request %s (server may reject parallel downloads)", resp.StatusCode, t.Range.BytesRange())
	}

	output, err := os.OpenFile(t.destPath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return errors.Wrapf(err, "failed to create: %q", t.String())
	}
	defer output.Close()

	rd := &barCountReader{
		Reader:   resp.Body,
		taskBar:  bar,
		totalBar: totalBar,
	}

	if _, err := io.Copy(output, rd); err != nil {
		return errors.Wrapf(err, "failed to write response body: %q", t.String())
	}

	return nil
}

type barCountReader struct {
	io.Reader
	taskBar  *mpb.Bar
	totalBar *mpb.Bar
}

func (r *barCountReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.taskBar.IncrBy(n)
		r.totalBar.IncrBy(n)
	}
	return n, err
}

func bindFiles(c *DownloadConfig, partialDir string) error {
	fmt.Fprintln(stdout, "\nbinding with files...")

	destPath := filepath.Join(c.Dirname, c.Filename)
	f, err := os.Create(destPath)
	if err != nil {
		return errors.Wrap(err, "failed to create a file in download location")
	}
	defer f.Close()

	p := newProgressContainer()
	bar := p.AddBar(c.ContentLength, barDecorators("Bind")...)

	copyFn := func(name string) error {
		subfp, err := os.Open(name)
		if err != nil {
			return errors.Wrapf(err, "failed to open %q in download location", name)
		}

		defer subfp.Close()

		proxy := bar.ProxyReader(subfp)
		if _, err := io.Copy(f, proxy); err != nil {
			proxy.Close()
			return errors.Wrapf(err, "failed to copy %q", name)
		}

		return proxy.Close()
	}

	for i := 0; i < c.Procs; i++ {
		partialFilename := getPartialFilePath(partialDir, c.Filename, c.Procs, i)
		if err := copyFn(partialFilename); err != nil {
			p.Shutdown()
			return err
		}

		// remove a file in download location for join
		if err := os.Remove(partialFilename); err != nil {
			p.Shutdown()
			return errors.Wrapf(err, "failed to remove %q in download location", partialFilename)
		}
	}

	p.Wait()

	// remove download location
	// RemoveAll reason: will create .DS_Store in download location if execute on mac
	if err := os.RemoveAll(partialDir); err != nil {
		return errors.Wrap(err, "failed to remove download location")
	}

	return nil
}
