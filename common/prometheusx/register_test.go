package prometheusx

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// TestRegisterReusesExistingCollector 校验同类型重复指标返回已有实例，不在运行期触发 panic。
func TestRegisterReusesExistingCollector(t *testing.T) {
	previousRegisterer := prometheus.DefaultRegisterer
	previousGatherer := prometheus.DefaultGatherer
	registry := prometheus.NewRegistry()
	prometheus.DefaultRegisterer = registry
	prometheus.DefaultGatherer = registry
	t.Cleanup(func() {
		prometheus.DefaultRegisterer = previousRegisterer
		prometheus.DefaultGatherer = previousGatherer
	})

	first := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_prometheusx_reuse_total", Help: "测试重复注册。"}, []string{"result"})
	registered, err := Register(first)
	if err != nil {
		t.Fatalf("首次 Register() error = %v", err)
	}
	duplicate := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_prometheusx_reuse_total", Help: "测试重复注册。"}, []string{"result"})
	reused, err := Register(duplicate)
	if err != nil {
		t.Fatalf("重复 Register() error = %v", err)
	}
	if reused != registered {
		t.Fatal("重复注册必须复用注册表中的既有 Collector")
	}

	conflictingCounter := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_prometheusx_conflict_total", Help: "测试类型冲突。"}, []string{"result"})
	if _, err := Register(conflictingCounter); err != nil {
		t.Fatalf("冲突指标首次 Register() error = %v", err)
	}
	conflictingGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_prometheusx_conflict_total", Help: "测试类型冲突。"}, []string{"result"})
	if _, err := Register(conflictingGauge); err == nil {
		t.Fatal("同名不同类型 Collector 必须返回启动错误")
	}
}
