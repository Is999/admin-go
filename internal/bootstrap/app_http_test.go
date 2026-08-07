package bootstrap

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/rest"
)

// TestLimitHTTPDrainClosesLongRequest 确保长请求不会阻塞后续资源关闭。
func TestLimitHTTPDrainClosesLongRequest(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	requestStarted := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(requestStarted)
		<-request.Context().Done()
	})}
	drainDone := make(chan struct{})
	defer close(drainDone)
	limitHTTPDrain(server, 30*time.Millisecond, drainDone)

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()
	go func() {
		_, _ = http.Get("http://" + listener.Addr().String())
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("long request did not start")
	}

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- server.Shutdown(context.Background())
	}()
	select {
	case err = <-shutdownDone:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("shutdown exceeded HTTP drain limit")
	}
	if err = <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("serve error = %v", err)
	}
}

// TestRunHTTPServersReturnsBindErrorAndClosesPeer 确保端口冲突返回 error，且已启动的同组监听器会被关闭。
func TestRunHTTPServersReturnsBindErrorAndClosesPeer(t *testing.T) {
	// go-zero 的监听器在端口绑定失败时仍会登记全局关闭回调；测试结束时用其测试专用入口清空回调，避免遗留等待协程。
	defer proc.Shutdown()
	goodPort := reserveHTTPTestPort(t)
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen occupied port: %v", err)
	}
	defer func() { _ = occupied.Close() }()
	badPort := occupied.Addr().(*net.TCPAddr).Port

	good := newHTTPServerRun("测试公网", "127.0.0.1", goodPort, newHTTPTestServer(t, goodPort))
	bad := newHTTPServerRun("测试内网", "127.0.0.1", badPort, newHTTPTestServer(t, badPort))
	if err = runHTTPServers([]*httpServerRun{good, bad}, 200*time.Millisecond); err == nil {
		t.Fatal("runHTTPServers() error = nil, want occupied port error")
	}
	connection, dialErr := net.DialTimeout("tcp", good.address, 100*time.Millisecond)
	if dialErr == nil {
		_ = connection.Close()
		t.Fatalf("peer listener %s is still reachable after startup failure", good.address)
	}
}

// reserveHTTPTestPort 获取一个当前空闲端口并立即释放，仅用于本进程启动测试。
func reserveHTTPTestPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release reserved port: %v", err)
	}
	return port
}

// newHTTPTestServer 创建只用于生命周期测试的最小 go-zero HTTP Server。
func newHTTPTestServer(t *testing.T, port int) *rest.Server {
	t.Helper()
	server, err := rest.NewServer(rest.RestConf{
		ServiceConf: service.ServiceConf{Name: "http-lifecycle-test"},
		Host:        "127.0.0.1",
		Port:        port,
	})
	if err != nil {
		t.Fatalf("rest.NewServer() error = %v", err)
	}
	return server
}
