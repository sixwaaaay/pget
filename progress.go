package pget

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/k0kubun/go-ansi"
	"github.com/schollz/progressbar/v3"
)

// ProgressIndicator tracks io progress with a terminal progress bar.
// The implementation follows http-downloader/pkg/net/progress.go.
type ProgressIndicator struct {
	Writer io.Writer
	Title  string
	Total  float64

	line int
	bar  *progressbar.ProgressBar
}

var (
	progressLine        int
	progressCurrentLine int
	progressGuard       sync.Mutex
)

func progressWriter() io.Writer {
	if f, ok := stdout.(*os.File); ok && f == os.Stdout {
		return ansi.NewAnsiStdout()
	}
	return stdout
}

// Init prepares the progress bar on its own terminal line.
func (i *ProgressIndicator) Init() {
	progressGuard.Lock()
	i.line = progressLine
	progressLine++
	progressGuard.Unlock()

	i.bar = progressbar.NewOptions64(int64(i.Total),
		progressbar.OptionSetWriter(progressWriter()),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(10),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetDescription(fmt.Sprintf("[cyan][reset] %s", i.Title)),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)
}

// Close finishes the progress bar.
func (i *ProgressIndicator) Close() {
	if i.bar != nil {
		_ = i.bar.Close()
	}
}

// Write updates the destination and the progress bar.
func (i *ProgressIndicator) Write(p []byte) (n int, err error) {
	progressGuard.Lock()
	defer progressGuard.Unlock()

	bias := progressCurrentLine - i.line
	progressCurrentLine = i.line
	if bias > 0 {
		fmt.Fprintf(stdout, "\r\033[%dA", bias)
	} else if bias < 0 {
		fmt.Fprintf(stdout, "\r\033[%dB", -bias)
	}

	return io.MultiWriter(i.Writer, i.bar).Write(p)
}
