package platform

import (
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/beck-8/subs-check/config"
	"github.com/juju/ratelimit"
	"github.com/metacubex/mihomo/common/convert"
)

// networkLimitedReader 基于网络层字节计数器的大小限制 reader
type networkLimitedReader struct {
	reader       io.Reader
	bytesCounter *uint64
	startBytes   uint64
	limit        uint64
}

func (r *networkLimitedReader) Read(p []byte) (n int, err error) {
	if r.limit > 0 {
		currentBytes := atomic.LoadUint64(r.bytesCounter)
		networkRead := currentBytes - r.startBytes

		if networkRead >= r.limit {
			return 0, io.EOF
		}

		// 限制本次读取的大小（粗略控制，因为网络层可能读取更多）
		if remaining := r.limit - networkRead; remaining < uint64(len(p)) {
			p = p[:remaining]
		}
	}
	return r.reader.Read(p)
}

// 测速提前截止参数：
// minEarlySample 达到该采样量、且 minEarlyWindow 观察期足够后，
// 实时速率明显达标/不达标就提前结束，避免所有节点都固定下载满 DownloadMB。
const (
	minEarlySample  = 1 * 1024 * 1024
	earlyCheckBytes = 256 * 1024
)

// minEarlyWindow 观察期,做成包级变量便于测试缩短。
var minEarlyWindow = 1500 * time.Millisecond

// CheckSpeed downloads speedTestURL through httpClient and returns the measured
// throughput. The URL is passed in explicitly (rather than read from
// config.GlobalConfig) so a run captured at pipeline start stays consistent
// even if the user edits SpeedTestUrl mid-check.
func CheckSpeed(httpClient *http.Client, bucket *ratelimit.Bucket, bytesCounter *uint64, speedTestURL string) (int, int64, error) {
	// 注意：速度限制在网络层（statsConn）实现，大小限制在应用层基于网络字节计数器实现
	// - 速度限制：通过 bucket 在 statsConn 中实现（网络层）
	// - 大小限制：通过 networkLimitedReader 基于网络字节计数器实现（应用层，但限制网络流量）

	// 创建一个新的测速专用客户端，基于原有客户端的传输层
	speedClient := &http.Client{
		// 设置更长的超时时间用于测速
		Timeout: time.Duration(config.GlobalConfig.DownloadTimeout) * time.Second,
		// 保持原有的传输层配置
		Transport: httpClient.Transport,
	}

	req, err := http.NewRequest("GET", speedTestURL, nil)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("User-Agent", convert.RandUserAgent())

	// 记录测速前的网络传输字节数
	var startBytes uint64
	if bytesCounter != nil {
		startBytes = *bytesCounter
	}
	startTime := time.Now()

	resp, err := speedClient.Do(req)
	if err != nil {
		slog.Debug(fmt.Sprintf("测速请求失败: %v", err))
		return 0, 0, err
	}
	defer resp.Body.Close()

	// 计算网络层的大小限制
	var limitSize uint64
	if config.GlobalConfig.DownloadMB > 0 {
		limitSize = uint64(config.GlobalConfig.DownloadMB) * 1024 * 1024
	} else {
		limitSize = 0 // 不限制
	}

	// 使用 networkLimitedReader 包装响应体，基于网络字节计数器限制大小
	limitedReader := &networkLimitedReader{
		reader:       resp.Body,
		bytesCounter: bytesCounter,
		startBytes:   startBytes,
		limit:        limitSize,
	}

	// 分块读取，周期性做提前截止判定。
	// 达标(overall >= min-speed)或整体与最近窗口都不达标时提前结束；
	// 临界节点不提前结束,仍会下载满 DownloadMB 以保证测速准确。
	minSpeed := config.GlobalConfig.MinSpeed
	buf := make([]byte, earlyCheckBytes)
	var totalBytes int64
	windowBytes := int64(0)
	lastWindow := time.Now()

	for {
		n, rerr := limitedReader.Read(buf)
		if n > 0 {
			totalBytes += int64(n)
			windowBytes += int64(n)
		}
		now := time.Now()
		if windowBytes >= earlyCheckBytes || rerr != nil {
			windowDur := now.Sub(lastWindow)
			var windowSpeed int
			if windowDur.Milliseconds() > 0 {
				windowSpeed = int(float64(windowBytes) / 1024 * 1000 / float64(windowDur.Milliseconds()))
			}
			elapsed := now.Sub(startTime)
			if totalBytes >= minEarlySample && elapsed >= minEarlyWindow {
				overall := int(float64(totalBytes) / 1024 * 1000 / float64(elapsed.Milliseconds()+1))
				if overall >= minSpeed {
					break // 明显达标,提前结束
				}
				if windowSpeed < minSpeed {
					break // 整体与最近窗口均不达标,提前结束
				}
			}
			windowBytes = 0
			lastWindow = now
		}
		if rerr != nil {
			break
		}
	}

	// 计算下载时间（毫秒）
	duration := time.Since(startTime).Milliseconds()
	if duration == 0 {
		duration = 1 // 避免除以零
	}

	// 计算实际网络传输的字节数（压缩数据）
	var actualBytes int64
	if bytesCounter != nil {
		actualBytes = int64(*bytesCounter - startBytes)
	} else {
		// 如果没有字节计数器，无法获取准确数据
		actualBytes = 0
	}

	// 计算速度（KB/s），使用实际网络传输的字节数
	speed := int(float64(actualBytes) / 1024 * 1000 / float64(duration))

	return speed, actualBytes, nil
}
