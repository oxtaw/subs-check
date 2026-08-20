package proxies

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// geoRT 按 host 分发响应,模拟四个地理查询端点
type geoRT struct {
	mu       sync.Mutex
	handlers map[string]func() (*http.Response, error)
}

func newGeoRT() *geoRT {
	return &geoRT{handlers: map[string]func() (*http.Response, error){}}
}

func (g *geoRT) RoundTrip(req *http.Request) (*http.Response, error) {
	g.mu.Lock()
	h, ok := g.handlers[req.URL.Host]
	g.mu.Unlock()
	if !ok {
		return nil, errors.New("unexpected host: " + req.URL.Host)
	}
	return h()
}

func geoBody(body string) func() (*http.Response, error) {
	return func() (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	}
}

func geoBlock(done chan struct{}) func() (*http.Response, error) {
	return func() (*http.Response, error) {
		<-done
		return nil, errors.New("blocked")
	}
}

func geoDelay(d time.Duration, body string) func() (*http.Response, error) {
	return func() (*http.Response, error) {
		time.Sleep(d)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	}
}

const (
	meResp   = `{"ip":"1.1.1.1","country_code":"US"}`
	ipinfo   = `{"ip":"2.2.2.2","country_code":"JP"}`
	cfResp   = "loc=FR\nip=3.3.3.3\n"
	edgeResp = `{"eo":{"geo":{"countryCodeAlpha2":"DE"},"clientIp":"4.4.4.4"}}`
)

func geoClient(t *testing.T, rt *geoRT) *http.Client {
	t.Helper()
	rt.handlers["ip.122911.xyz"] = geoBody(meResp)
	rt.handlers["api.ipinfo.io"] = geoBody(ipinfo)
	rt.handlers["www.cloudflare.com"] = geoBody(cfResp)
	rt.handlers["functions-geolocation.edgeone.app"] = geoBody(edgeResp)
	return &http.Client{Transport: rt}
}

func TestGetProxyCountryFirstComeFirstServed(t *testing.T) {
	old := countryGrace
	countryGrace = 30 * time.Millisecond
	defer func() { countryGrace = old }()

	blocked := make(chan struct{})
	rt := newGeoRT()
	client := geoClient(t, rt)
	rt.handlers["ip.122911.xyz"] = geoBlock(blocked)
	rt.handlers["api.ipinfo.io"] = geoBlock(blocked)
	rt.handlers["www.cloudflare.com"] = geoBlock(blocked)

	start := time.Now()
	loc, ip := GetProxyCountry(client)
	if loc != "DE" || ip != "4.4.4.4" {
		t.Fatalf("先到先得应返回 edgeone 结果, 实际 loc=%q ip=%q", loc, ip)
	}
	// 其余三个端点被阻塞,旧实现会永远等它们,新实现应立即返回
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("应提前返回, 实际耗时 %v", elapsed)
	}
}

func TestGetProxyCountryPrefersHighPriorityWithinGrace(t *testing.T) {
	old := countryGrace
	countryGrace = 300 * time.Millisecond
	defer func() { countryGrace = old }()

	rt := newGeoRT()
	client := geoClient(t, rt)
	rt.handlers["ip.122911.xyz"] = geoDelay(50*time.Millisecond, meResp)

	loc, ip := GetProxyCountry(client)
	if loc != "US" || ip != "1.1.1.1" {
		t.Fatalf("宽限期内应优先返回 GetMe, 实际 loc=%q ip=%q", loc, ip)
	}
}

func TestGetProxyCountryLowPriorityWinsAfterGrace(t *testing.T) {
	old := countryGrace
	countryGrace = 100 * time.Millisecond
	defer func() { countryGrace = old }()

	rt := newGeoRT()
	client := geoClient(t, rt)
	// GetMe 慢于宽限期,低优先级的 GetIpinfo 应胜出
	rt.handlers["ip.122911.xyz"] = geoDelay(800*time.Millisecond, meResp)
	rt.handlers["www.cloudflare.com"] = geoBlock(make(chan struct{}))
	rt.handlers["functions-geolocation.edgeone.app"] = geoBlock(make(chan struct{}))

	start := time.Now()
	loc, ip := GetProxyCountry(client)
	if loc != "JP" || ip != "2.2.2.2" {
		t.Fatalf("应返回 GetIpinfo, 实际 loc=%q ip=%q", loc, ip)
	}
	if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
		t.Fatalf("不应等慢速的 GetMe, 实际耗时 %v", elapsed)
	}
}

func TestGetProxyCountryAllFail(t *testing.T) {
	old := countryGrace
	countryGrace = 30 * time.Millisecond
	defer func() { countryGrace = old }()

	rt := newGeoRT()
	client := geoClient(t, rt)
	for host := range rt.handlers {
		rt.handlers[host] = func() (*http.Response, error) { return nil, errors.New("err") }
	}

	loc, ip := GetProxyCountry(client)
	if loc != "" || ip != "" {
		t.Fatalf("全部失败应返回空, 实际 loc=%q ip=%q", loc, ip)
	}
}
