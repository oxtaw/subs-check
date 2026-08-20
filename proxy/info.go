package proxies

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/metacubex/mihomo/common/convert"
)

// 这里需要一个不限流的ipv4的非CF的API
// 因为ipv6在数据库中没有记载时会变成US。
// 不能用CF的API是因为我们要保留CF的节点（无proxyip的）
// GetProxyCountry 并行请求所有 IP 查询端点，先到先得：
// 最高优先级(GetMe)命中立即返回；低优先级先到时，在宽限期(grace)内
// 等待更高优先级结果再返回，避免像旧实现那样无谓阻塞到最慢端点。
// countryGrace 为包级变量便于测试缩短。
var countryGrace = 300 * time.Millisecond

func GetProxyCountry(httpClient *http.Client) (loc string, ip string) {
	// 顺序代表优先级，索引越小质量越高
	checkers := []func(*http.Client) (string, string){
		GetMe, GetIpinfo, GetCFProxy, GetEdgeOneProxy,
	}

	type result struct {
		idx int
		loc string
		ip  string
	}
	ch := make(chan result, len(checkers))
	var wg sync.WaitGroup
	for idx, fn := range checkers {
		wg.Add(1)
		go func(i int, f func(*http.Client) (string, string)) {
			defer wg.Done()
			l, p := f(httpClient)
			if l != "" && p != "" {
				ch <- result{i, l, p}
			}
		}(idx, fn)
	}
	go func() { wg.Wait(); close(ch) }()

	bestIdx := len(checkers)
	var bestLoc, bestIP string
	haveResult := false
	deadline := time.Now().Add(countryGrace)
	for {
		if bestIdx == 0 {
			return bestLoc, bestIP
		}
		var timerC <-chan time.Time
		if haveResult {
			timerC = time.After(time.Until(deadline))
		}
		select {
		case r, ok := <-ch:
			if !ok {
				// 所有端点都已返回且无更高优先级可等
				return bestLoc, bestIP
			}
			if !haveResult {
				haveResult = true
				deadline = time.Now().Add(countryGrace)
			}
			if r.idx < bestIdx {
				bestIdx, bestLoc, bestIP = r.idx, r.loc, r.ip
			}
		case <-timerC:
			// 已有低优先级结果且宽限期到，返回当前最优
			return bestLoc, bestIP
		}
	}
}

// GetEdgeOneProxy 通过腾讯 EdgeOne 获取地理位置
func GetEdgeOneProxy(httpClient *http.Client) (loc string, ip string) {
	type GeoResponse struct {
		Eo struct {
			Geo struct {
				CountryCodeAlpha2 string `json:"countryCodeAlpha2"`
			} `json:"geo"`
			ClientIp string `json:"clientIp"`
		} `json:"eo"`
	}

	url := "https://functions-geolocation.edgeone.app/geo"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		slog.Debug(fmt.Sprintf("创建请求失败: %s", err))
		return
	}
	req.Header.Set("User-Agent", convert.RandUserAgent())
	resp, err := httpClient.Do(req)
	if err != nil {
		slog.Debug(fmt.Sprintf("edgeone获取节点位置失败: %s", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Debug(fmt.Sprintf("edgeone返回非200状态码: %v", resp.StatusCode))
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf("edgeone读取节点位置失败: %s", err))
		return
	}

	var eo GeoResponse
	err = json.Unmarshal(body, &eo)
	if err != nil {
		slog.Debug(fmt.Sprintf("解析edgeone JSON 失败: %v", err))
		return
	}

	return eo.Eo.Geo.CountryCodeAlpha2, eo.Eo.ClientIp
}

// GetCFProxy 通过 Cloudflare cdn-cgi/trace 获取地理位置
// 局限：CF 节点需要 proxyip 落地才能访问套 CF 的网站，trace 返回的是 proxyip 落地位置而非节点真实出口位置
func GetCFProxy(httpClient *http.Client) (loc string, ip string) {
	url := "https://www.cloudflare.com/cdn-cgi/trace"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		slog.Debug(fmt.Sprintf("创建请求失败: %s", err))
		return
	}
	req.Header.Set("User-Agent", convert.RandUserAgent())
	resp, err := httpClient.Do(req)
	if err != nil {
		slog.Debug(fmt.Sprintf("cf获取节点位置失败: %s", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Debug(fmt.Sprintf("cf返回非200状态码: %v", resp.StatusCode))
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf("cf读取节点位置失败: %s", err))
		return
	}

	// Parse the response text to find loc=XX
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "loc=") {
			loc = strings.TrimPrefix(line, "loc=")
		}
		if strings.HasPrefix(line, "ip=") {
			ip = strings.TrimPrefix(line, "ip=")
		}
	}
	return
}

// GetIPSB 通过 ip.sb 获取地理位置
func GetIPSB(httpClient *http.Client) (loc string, ip string) {
	type GeoIPData struct {
		IP      string `json:"ip"`
		Country string `json:"country_code"`
	}

	url := "https://api.ip.sb/geoip"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		slog.Debug(fmt.Sprintf("创建请求失败: %s", err))
		return
	}
	req.Header.Set("User-Agent", convert.RandUserAgent())
	resp, err := httpClient.Do(req)
	if err != nil {
		slog.Debug(fmt.Sprintf("ip.sb获取节点位置失败: %s", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Debug(fmt.Sprintf("ip.sb返回非200状态码: %v", resp.StatusCode))
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf("ip.sb读取节点位置失败: %s", err))
		return
	}

	var geo GeoIPData
	err = json.Unmarshal(body, &geo)
	if err != nil {
		slog.Debug(fmt.Sprintf("解析ip.sb JSON 失败: %v", err))
		return
	}

	return geo.Country, geo.IP
}

func GetMe(httpClient *http.Client) (loc string, ip string) {
	type GeoIPData struct {
		IP      string `json:"ip"`
		Country string `json:"country_code"`
	}

	url := "https://ip.122911.xyz/api/ipinfo"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		slog.Debug(fmt.Sprintf("创建请求失败: %s", err))
		return
	}
	req.Header.Set("User-Agent", "subs-check (https://github.com/beck-8/subs-check)")
	resp, err := httpClient.Do(req)
	if err != nil {
		slog.Debug(fmt.Sprintf("me获取节点位置失败: %s", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Debug(fmt.Sprintf("me返回非200状态码: %v", resp.StatusCode))
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf("me读取节点位置失败: %s", err))
		return
	}

	var geo GeoIPData
	err = json.Unmarshal(body, &geo)
	if err != nil {
		slog.Debug(fmt.Sprintf("解析me JSON 失败: %v", err))
		return
	}

	return geo.Country, geo.IP
}

func GetIpinfo(httpClient *http.Client) (loc string, ip string) {
	type GeoIPData struct {
		IP      string `json:"ip"`
		Country string `json:"country_code"`
	}

	url := string([]byte{104, 116, 116, 112, 115, 58, 47, 47, 97, 112, 105, 46, 105, 112,
		105, 110, 102, 111, 46, 105, 111, 47, 108, 105, 116, 101, 47, 109, 101, 63, 116,
		111, 107, 101, 110, 61, 48, 57, 48, 102, 54, 54, 55, 55, 57, 55, 51, 51, 98, 102})
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		slog.Debug(fmt.Sprintf("创建请求失败: %s", err))
		return
	}
	req.Header.Set("User-Agent", "subs-check (https://github.com/beck-8/subs-check)")
	resp, err := httpClient.Do(req)
	if err != nil {
		slog.Debug(fmt.Sprintf("Ipinfo获取节点位置失败: %s", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Debug(fmt.Sprintf("Ipinfo返回非200状态码: %v", resp.StatusCode))
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf("Ipinfo读取节点位置失败: %s", err))
		return
	}

	var geo GeoIPData
	err = json.Unmarshal(body, &geo)
	if err != nil {
		slog.Debug(fmt.Sprintf("解析Ipinfo JSON 失败: %v", err))
		return
	}

	return geo.Country, geo.IP
}
