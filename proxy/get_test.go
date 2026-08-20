package proxies

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/beck-8/subs-check/config"
)

func TestGetDateFromSubsReusesTransport(t *testing.T) {
	oldDNS := config.GlobalConfig.DNS.Enable
	oldUA := config.GlobalConfig.SubUrlsGetUA
	oldRetry := config.GlobalConfig.SubUrlsReTry
	oldTimeout := config.GlobalConfig.SubUrlsTimeout
	defer func() {
		config.GlobalConfig.DNS.Enable = oldDNS
		config.GlobalConfig.SubUrlsGetUA = oldUA
		config.GlobalConfig.SubUrlsReTry = oldRetry
		config.GlobalConfig.SubUrlsTimeout = oldTimeout
	}()
	config.GlobalConfig.DNS.Enable = false
	config.GlobalConfig.SubUrlsGetUA = "subs-check-test"
	config.GlobalConfig.SubUrlsReTry = 1
	config.GlobalConfig.SubUrlsTimeout = 5

	var newConns int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("proxies: []"))
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			atomic.AddInt32(&newConns, 1)
		}
	}
	srv.Start()
	defer srv.Close()

	// 重置共享 transport,避免受其它测试影响
	subTransportMu.Lock()
	subTransport = nil
	subTransportDNS = false
	subTransportMu.Unlock()

	for i := 0; i < 5; i++ {
		data, err := GetDateFromSubs(srv.URL)
		if err != nil {
			t.Fatalf("GetDateFromSubs: %v", err)
		}
		if len(data) == 0 {
			t.Fatal("空响应")
		}
	}
	if n := atomic.LoadInt32(&newConns); n != 1 {
		t.Fatalf("应复用同一连接(keep-alive), 实际新建连接数=%d", n)
	}
}
