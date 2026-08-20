package platform

import (
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beck-8/subs-check/config"
)

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// rateReader 按 bps 限速的 reader,用于模拟固定速率下载
type rateReader struct {
	r         io.Reader
	bps       int64
	last      time.Time
	delivered int64
}

func (r *rateReader) Read(p []byte) (int, error) {
	if len(p) > 65536 {
		p = p[:65536]
	}
	n, err := r.r.Read(p)
	if n > 0 {
		r.delivered += int64(n)
	}
	if r.bps > 0 {
		want := time.Duration(float64(r.delivered) / float64(r.bps) * float64(time.Second))
		if elapsed := time.Since(r.last); want > elapsed {
			time.Sleep(want - elapsed)
		}
	}
	return n, err
}

// countingBody 模拟网络层字节计数器:每次应用层读取都累加计数
type countingBody struct {
	r io.Reader
	c *uint64
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if n > 0 {
		atomic.AddUint64(b.c, uint64(n))
	}
	return n, err
}

func (b *countingBody) Close() error { return nil }

type speedRT struct {
	body    io.Reader
	counter *uint64
}

func (rt *speedRT) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       &countingBody{r: rt.body, c: rt.counter},
	}, nil
}

func TestCheckSpeedEarlyExit(t *testing.T) {
	oldWindow := minEarlyWindow
	oldTimeout := config.GlobalConfig.DownloadTimeout
	oldMB := config.GlobalConfig.DownloadMB
	oldMin := config.GlobalConfig.MinSpeed
	defer func() {
		minEarlyWindow = oldWindow
		config.GlobalConfig.DownloadTimeout = oldTimeout
		config.GlobalConfig.DownloadMB = oldMB
		config.GlobalConfig.MinSpeed = oldMin
	}()
	minEarlyWindow = 200 * time.Millisecond
	config.GlobalConfig.DownloadTimeout = 5

	// run 用限速为 rateBPS 的流式 body 执行一次测速
	run := func(rateBPS int64, minSpeed, downloadMB int) (int, int64, error) {
		var counter uint64
		body := &rateReader{
			r:    io.LimitReader(zeroReader{}, 64*1024*1024),
			bps:  rateBPS,
			last: time.Now(),
		}
		client := &http.Client{Transport: &speedRT{body: body, counter: &counter}}
		config.GlobalConfig.MinSpeed = minSpeed
		config.GlobalConfig.DownloadMB = downloadMB
		return CheckSpeed(client, nil, &counter, "http://example.invalid/dl")
	}

	t.Run("达标提前结束", func(t *testing.T) {
		start := time.Now()
		speed, actual, err := run(4*1024*1024, 512, 50)
		if err != nil {
			t.Fatalf("CheckSpeed: %v", err)
		}
		if speed < 512 {
			t.Fatalf("应判定达标, speed=%d KB/s", speed)
		}
		if actual >= 50*1024*1024 {
			t.Fatalf("应提前结束而非下载满50MB, actual=%d", actual)
		}
		if time.Since(start) > 3*time.Second {
			t.Fatalf("应提前结束, 耗时过长 %v", time.Since(start))
		}
	})

	t.Run("不达标提前结束", func(t *testing.T) {
		speed, actual, err := run(4*1024*1024, 100000, 50)
		if err != nil {
			t.Fatalf("CheckSpeed: %v", err)
		}
		if speed >= 100000 {
			t.Fatalf("应判定不达标, speed=%d KB/s", speed)
		}
		if actual >= 50*1024*1024 {
			t.Fatalf("应提前结束而非下载满50MB, actual=%d", actual)
		}
	})

	t.Run("下载满download-mb限制", func(t *testing.T) {
		_, actual, err := run(0, 0, 2)
		if err != nil {
			t.Fatalf("CheckSpeed: %v", err)
		}
		if actual != 2*1024*1024 {
			t.Fatalf("应下载满2MB限制, actual=%d", actual)
		}
	})
}
