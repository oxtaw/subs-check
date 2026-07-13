package proxies

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	u "net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/beck-8/subs-check/config"
	"github.com/beck-8/subs-check/utils"
	"github.com/metacubex/mihomo/common/convert"
	"github.com/metacubex/mihomo/component/resolver"
	"github.com/samber/lo"
	"gopkg.in/yaml.v3"
)

type subEntry struct {
	url    string
	source string
}

func GetProxies() ([]map[string]any, error) {

	// 解析本地与远程订阅清单
	subUrls, localNum, remoteNum := resolveSubUrls()
	slog.Info("订阅链接数量", "本地", localNum, "远程", remoteNum, "总计", len(subUrls))

	if len(config.GlobalConfig.NodeType) > 0 {
		slog.Info("只筛选用户设置的协议", "type", config.GlobalConfig.NodeType)
	}

	var wg sync.WaitGroup
	// Subscription-fetch concurrency is decoupled from the alive-check
	// concurrency: most users want dozens of cheap HTTP fetches in parallel
	// even when the alive-check pool is tuned large. Falls back to 20 if
	// the user hasn't set it (zero value).
	subFetchConcurrency := config.GlobalConfig.SubUrlsConcurrent
	if subFetchConcurrency <= 0 {
		subFetchConcurrency = 20
	}
	concurrentLimit := make(chan struct{}, subFetchConcurrency) // 限制并发数

	// 按订阅顺序预分配槽位,每个 goroutine 只写自己的下标,无竞争
	// 这样即便是并发获取,最终合并时仍能保持 subUrls 的顺序(本地在前,远程在后)
	buckets := make([][]map[string]any, len(subUrls))

	// 启动工作协程
	for idx, subUrl := range subUrls {
		wg.Add(1)
		concurrentLimit <- struct{}{} // 获取令牌

		go func(i int, e subEntry) {
			defer wg.Done()
			defer func() { <-concurrentLimit }() // 释放令牌

			url := strings.TrimSpace(e.url)
			candidates := utils.WarpUrls(url)
			data, fetchedURL, fetchedVia, err := GetDateFromSubsAny(candidates)
			if err != nil {
				slog.Error("获取订阅链接错误跳过", "source", e.source, "url", url, "err", err)
				return
			}
			if fetchedURL != "" && len(candidates) > 0 && fetchedURL != candidates[0] {
				slog.Warn("订阅链接使用备用地址获取成功", "source", e.source, "url", url, "fetched_url", fetchedURL, "via", fetchedVia)
			} else if fetchedVia == "direct" {
				slog.Warn("订阅链接代理失败后直连获取成功", "source", e.source, "url", url, "fetched_url", fetchedURL)
			}

			var tag string
			if d, err := u.Parse(url); err == nil {
				tag = d.Fragment
			}

			var local []map[string]any

			var con map[string]any
			err = yaml.Unmarshal(data, &con)
			if err != nil {
				proxyList, err := convert.ConvertsV2Ray(data)
				if err != nil {
					slog.Error("解析proxy错误", "source", e.source, "url", url, "err", err)
					return
				}
				slog.Debug("获取订阅链接", "source", e.source, "url", url, "fetched_url", fetchedURL, "via", fetchedVia, "count", len(proxyList))
				local = make([]map[string]any, 0, len(proxyList))
				for _, proxy := range proxyList {
					// 只测试指定协议
					if t, ok := proxy["type"].(string); ok {
						if len(config.GlobalConfig.NodeType) > 0 && !lo.Contains(config.GlobalConfig.NodeType, t) {
							continue
						}
					}

					// 为每个节点添加订阅链接来源信息和备注
					proxy["sub_url"] = url
					if tag != "" {
						proxy["sub_tag"] = tag
					}
					local = append(local, proxy)
				}
				buckets[i] = local
				return
			}

			proxyInterface, ok := con["proxies"]
			if !ok || proxyInterface == nil {
				slog.Error("订阅链接没有proxies", "source", e.source, "url", url)
				return
			}

			proxyList, ok := proxyInterface.([]any)
			if !ok {
				return
			}
			slog.Debug("获取订阅链接", "source", e.source, "url", url, "fetched_url", fetchedURL, "via", fetchedVia, "count", len(proxyList))
			local = make([]map[string]any, 0, len(proxyList))
			for _, proxy := range proxyList {
				if proxyMap, ok := proxy.(map[string]any); ok {
					if t, ok := proxyMap["type"].(string); ok {
						// 只测试指定协议
						if len(config.GlobalConfig.NodeType) > 0 && !lo.Contains(config.GlobalConfig.NodeType, t) {
							continue
						}
						// 虽然支持mihomo支持下划线，但是这里为了规范，还是改成横杠
						// todo: 不知道后边还有没有这类问题
						switch t {
						case "hysteria2", "hy2":
							if _, ok := proxyMap["obfs_password"]; ok {
								proxyMap["obfs-password"] = proxyMap["obfs_password"]
								delete(proxyMap, "obfs_password")
							}
						}
					}
					// 为每个节点添加订阅链接来源信息和备注
					proxyMap["sub_url"] = url
					if tag != "" {
						proxyMap["sub_tag"] = tag
					}
					local = append(local, proxyMap)
				}
			}
			buckets[i] = local
		}(idx, subUrl)
	}

	// 等待所有工作协程完成
	wg.Wait()

	// 按订阅顺序合并,保证本地订阅在前、远程订阅在后,订阅内节点顺序也保留
	total := 0
	for _, b := range buckets {
		total += len(b)
	}
	mihomoProxies := make([]map[string]any, 0, total)
	for _, b := range buckets {
		mihomoProxies = append(mihomoProxies, b...)
	}

	return mihomoProxies, nil
}

// from 3k
// resolveSubUrls 合并本地与远程订阅清单并去重
func resolveSubUrls() ([]subEntry, int, int) {
	// 计数
	var localNum, remoteNum int
	localNum = len(config.GlobalConfig.SubUrls)

	entries := make([]subEntry, 0, len(config.GlobalConfig.SubUrls))
	// 本地配置
	for _, u := range config.GlobalConfig.SubUrls {
		entries = append(entries, subEntry{url: u, source: "本地配置"})
	}

	// 远程清单
	if len(config.GlobalConfig.SubUrlsRemote) != 0 {
		for _, d := range config.GlobalConfig.SubUrlsRemote {
			if remote, err := fetchRemoteSubUrls(d); err != nil {
				slog.Warn("获取远程订阅清单失败，已忽略", "url", d, "err", err)
			} else {
				remoteNum += len(remote)
				for _, u := range remote {
					entries = append(entries, subEntry{url: u, source: d})
				}
			}
		}
	}

	// 规范化与去重
	seen := make(map[string]struct{}, len(entries))
	out := make([]subEntry, 0, len(entries))
	for _, e := range entries {
		s := strings.TrimSpace(e.url)
		if s == "" || strings.HasPrefix(s, "#") { // 跳过空行与注释
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, subEntry{url: s, source: e.source})
	}
	return out, localNum, remoteNum
}

// fetchRemoteSubUrls 从远程地址读取订阅URL清单
// 支持两种格式：
// 1) 纯文本，按换行分隔，支持以 # 开头的注释与空行
// 2) YAML/JSON 的字符串数组
func fetchRemoteSubUrls(listURL string) ([]string, error) {
	if listURL == "" {
		return nil, errors.New("empty list url")
	}
	candidates := utils.WarpUrls(listURL)
	data, fetchedURL, fetchedVia, err := GetDateFromSubsAny(candidates)
	if err != nil {
		return nil, err
	}
	if fetchedURL != "" && len(candidates) > 0 && fetchedURL != candidates[0] {
		slog.Warn("远程订阅清单使用备用地址获取成功", "url", listURL, "fetched_url", fetchedURL, "via", fetchedVia)
	} else if fetchedVia == "direct" {
		slog.Warn("远程订阅清单代理失败后直连获取成功", "url", listURL, "fetched_url", fetchedURL)
	}

	// 优先尝试解析为字符串数组（YAML/JSON兼容）
	var arr []string
	if err := yaml.Unmarshal(data, &arr); err == nil && len(arr) > 0 {
		return arr, nil
	}

	// 回退为按行解析
	res := make([]string, 0, 16)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		res = append(res, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

// subClient 共享的 HTTP 客户端，复用连接池，避免每个订阅URL都创建新连接
var subClient *http.Client
var subDirectClient *http.Client

type subFetchPlan struct {
	url    string
	via    string
	client *http.Client
}

func getSubClient() *http.Client {
	if subClient != nil {
		return subClient
	}
	subClient = newSubClient(true)
	return subClient
}

func getSubDirectClient() *http.Client {
	if subDirectClient != nil {
		return subDirectClient
	}
	subDirectClient = newSubClient(false)
	return subDirectClient
}

func newSubClient(useProxy bool) *http.Client {
	timeout := config.GlobalConfig.SubUrlsTimeout
	if timeout == 0 {
		timeout = 30
	}
	dialTimeout := time.Duration(timeout) * time.Second
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   time.Duration(timeout) * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if useProxy {
		transport.Proxy = http.ProxyFromEnvironment
	}
	// Route DNS through the configured mihomo resolver so subscription domains aren't leaked to system DNS.
	// Only when user enabled custom DNS — keeps default behavior unchanged for existing users.
	if config.GlobalConfig.DNS.Enable {
		transport.DialContext = newMihomoDialer(dialTimeout)
	}
	return &http.Client{
		Timeout:   time.Duration(timeout) * time.Second,
		Transport: transport,
	}
}

// 订阅链接中获取数据
func GetDateFromSubs(subUrl string) ([]byte, error) {
	data, _, _, err := GetDateFromSubsAny([]string{subUrl})
	return data, err
}

func GetDateFromSubsAny(subUrls []string) ([]byte, string, string, error) {
	plans := buildSubFetchPlans(subUrls)
	var errs []string

	maxRetries := config.GlobalConfig.SubUrlsReTry
	if maxRetries <= 0 {
		maxRetries = 3
	}
	retryInterval := config.GlobalConfig.SubUrlsRetryInterval
	if retryInterval == 0 {
		retryInterval = 3
	}

	for i := range maxRetries {
		if i > 0 {
			backoff := retryInterval * (1 << (i - 1))
			time.Sleep(time.Duration(backoff) * time.Second)
		}

		for _, plan := range plans {
			data, err := getDateFromSubURL(plan.client, plan.url)
			if err == nil {
				return data, plan.url, plan.via, nil
			}
			errs = append(errs, fmt.Sprintf("%s(%s): %v", plan.url, plan.via, err))
		}
	}
	return nil, "", "", fmt.Errorf("所有候选地址失败: %s", strings.Join(errs, "; "))
}

func buildSubFetchPlans(subUrls []string) []subFetchPlan {
	hasProxy := hasSubscriptionProxy()
	defaultVia := "direct"
	if hasProxy {
		defaultVia = "proxy"
	}

	plans := make([]subFetchPlan, 0, len(subUrls)*2)
	add := func(subUrl, via string, client *http.Client) {
		for _, plan := range plans {
			if plan.url == subUrl && plan.via == via {
				return
			}
		}
		plans = append(plans, subFetchPlan{url: subUrl, via: via, client: client})
	}

	if len(subUrls) == 0 {
		return plans
	}

	add(subUrls[0], defaultVia, getSubClient())
	if hasProxy {
		for _, subUrl := range subUrls {
			if isJsDelivrURL(subUrl) {
				add(subUrl, "direct", getSubDirectClient())
			}
		}
	}
	for _, subUrl := range subUrls[1:] {
		add(subUrl, defaultVia, getSubClient())
	}
	if hasProxy {
		for _, subUrl := range subUrls {
			if !isJsDelivrURL(subUrl) {
				add(subUrl, "direct", getSubDirectClient())
			}
		}
	}
	return plans
}

func isJsDelivrURL(rawURL string) bool {
	parsed, err := u.Parse(rawURL)
	return err == nil && parsed.Host == "cdn.jsdelivr.net"
}

func hasSubscriptionProxy() bool {
	if config.GlobalConfig.Proxy != "" {
		return true
	}
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"} {
		if strings.TrimSpace(getenv(key)) != "" {
			return true
		}
	}
	return false
}

var getenv = func(key string) string {
	return os.Getenv(key)
}

func getDateFromSubURL(client *http.Client, subUrl string) ([]byte, error) {
	req, err := http.NewRequest("GET", subUrl, nil)
	if err != nil {
		return nil, err
	}

	if config.GlobalConfig.SubUrlsGetUA == "random" {
		req.Header.Set("User-Agent", convert.RandUserAgent())
	} else {
		req.Header.Set("User-Agent", config.GlobalConfig.SubUrlsGetUA)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, fmt.Errorf("返回状态码: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("读取响应数据错误: %w", err)
	}
	return body, nil
}

// newMihomoDialer returns a DialContext that resolves via mihomo's global resolver
// (DoH when configured) then dials the resulting IP directly, avoiding the OS DNS path.
// dialTimeout is shared with the request-level timeout from SubUrlsTimeout.
func newMihomoDialer(dialTimeout time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	d := &net.Dialer{Timeout: dialTimeout}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		// If addr is already an IP, skip resolution.
		if ip := net.ParseIP(host); ip != nil {
			return d.DialContext(ctx, network, addr)
		}
		ips, err := resolver.LookupIP(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("resolve %s: %w", host, err)
		}
		if len(ips) == 0 {
			return nil, fmt.Errorf("no ip for %s", host)
		}
		// Try each IP in turn, returning on the first successful connection.
		var dialErr error
		for _, ip := range ips {
			conn, err := d.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return conn, nil
			}
			dialErr = err
		}
		return nil, fmt.Errorf("dial %s: %w", host, dialErr)
	}
}
