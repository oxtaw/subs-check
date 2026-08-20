package utils

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beck-8/subs-check/config"
)

// mockMihomoClient 记录并发度与调用次数
type mockMihomoClient struct {
	mu          sync.Mutex
	inFlight    int
	maxInFlight int
	calls       int
	failOn      string
}

func (m *mockMihomoClient) Do(req *http.Request) (*http.Response, error) {
	m.mu.Lock()
	m.inFlight++
	if m.inFlight > m.maxInFlight {
		m.maxInFlight = m.inFlight
	}
	m.calls++
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.inFlight--
		m.mu.Unlock()
	}()

	// 留出并发重叠窗口
	time.Sleep(10 * time.Millisecond)

	name := req.URL.Path[strings.LastIndex(req.URL.Path, "/")+1:]
	if name == m.failOn {
		return nil, errors.New("boom")
	}
	return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
}

func TestUpdateSubsConcurrent(t *testing.T) {
	old := config.GlobalConfig.MihomoApiUrl
	defer func() { config.GlobalConfig.MihomoApiUrl = old }()
	config.GlobalConfig.MihomoApiUrl = "http://127.0.0.1:1"

	names := make([]string, 0, 12)
	for i := 0; i < 12; i++ {
		names = append(names, fmt.Sprintf("sub%d", i))
	}

	client := &mockMihomoClient{}
	if err := updateSubs(client, names); err != nil {
		t.Fatalf("updateSubs: %v", err)
	}
	if client.calls != len(names) {
		t.Fatalf("应更新全部订阅, calls=%d want=%d", client.calls, len(names))
	}
	if client.maxInFlight > 5 {
		t.Fatalf("并发数应<=5, 实际=%d", client.maxInFlight)
	}
	if client.maxInFlight < 2 {
		t.Fatalf("应存在并发, 实际 maxInFlight=%d", client.maxInFlight)
	}
}

func TestUpdateSubsSingleFailureDoesNotStopOthers(t *testing.T) {
	old := config.GlobalConfig.MihomoApiUrl
	defer func() { config.GlobalConfig.MihomoApiUrl = old }()
	config.GlobalConfig.MihomoApiUrl = "http://127.0.0.1:1"

	names := []string{"ok1", "bad", "ok2", "ok3"}
	client := &mockMihomoClient{failOn: "bad"}
	if err := updateSubs(client, names); err != nil {
		t.Fatalf("updateSubs 不应返回错误: %v", err)
	}
	if client.calls != len(names) {
		t.Fatalf("单个失败不应影响其它订阅, calls=%d want=%d", client.calls, len(names))
	}
}
