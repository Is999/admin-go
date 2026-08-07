package bootstrap

import (
	"context"
	"reflect"
	"testing"
	"time"

	"admin/internal/bootstrap/components"
	"admin/internal/config"
	"admin/internal/svc"
)

// TestAppLifecycleHooksOrder 确保启动钩子按注册顺序执行，停止钩子按逆序执行。
func TestAppLifecycleHooksOrder(t *testing.T) {
	called := make([]string, 0, 4)
	app := &App{
		startHooks: []components.LifecycleHook{
			{Name: "first", Run: func(context.Context) error {
				called = append(called, "start:first")
				return nil
			}},
			{Name: "second", Run: func(context.Context) error {
				called = append(called, "start:second")
				return nil
			}},
		},
		stopHooks: []components.LifecycleHook{
			{Name: "first", Run: func(context.Context) error {
				called = append(called, "stop:first")
				return nil
			}},
			{Name: "second", Run: func(context.Context) error {
				called = append(called, "stop:second")
				return nil
			}},
		},
	}

	if err := app.runStartHooks(context.Background()); err != nil {
		t.Fatalf("启动生命周期钩子失败: %v", err)
	}
	if err := app.runStopHooks(context.Background()); err != nil {
		t.Fatalf("停止生命周期钩子失败: %v", err)
	}
	want := []string{"start:first", "start:second", "stop:second", "stop:first"}
	if !reflect.DeepEqual(called, want) {
		t.Fatalf("期望生命周期顺序为 %+v，实际为 %+v", want, called)
	}
}

// TestAppLifecycleHooksRollbackOnStartFailure 确保启动中途失败时会回滚已启动组件，避免后台协程残留。
func TestAppLifecycleHooksRollbackOnStartFailure(t *testing.T) {
	called := make([]string, 0, 3)
	alerter := &lifecycleAlertRecorder{}
	app := &App{
		ServiceContext: &svc.ServiceContext{RuntimeAlerter: alerter},
		startHooks: []components.LifecycleHook{
			{Name: "first", Run: func(context.Context) error {
				called = append(called, "start:first")
				return nil
			}},
			{Name: "second", Run: func(context.Context) error {
				called = append(called, "start:second")
				return assertLifecycleError{}
			}},
		},
		stopHooks: []components.LifecycleHook{
			{Name: "first", Run: func(context.Context) error {
				called = append(called, "stop:first")
				return nil
			}},
			{Name: "second", Run: func(context.Context) error {
				called = append(called, "stop:second")
				return nil
			}},
		},
	}

	if err := app.runStartHooks(context.Background()); err == nil {
		t.Fatal("期望启动失败返回错误，实际为 nil")
	}
	want := []string{"start:first", "start:second", "stop:first"}
	if !reflect.DeepEqual(called, want) {
		t.Fatalf("期望启动失败回滚顺序为 %+v，实际为 %+v", want, called)
	}
	if err := app.runStopHooks(context.Background()); err != nil {
		t.Fatalf("期望重复停止保持幂等，实际错误为 %v", err)
	}
	if len(alerter.alerts) != 1 || alerter.alerts[0].Operation != "start" || alerter.alerts[0].TaskName != "second" {
		t.Fatalf("启动失败告警不符合预期: %+v", alerter.alerts)
	}
}

// TestComponentRegisterFailureUsesIndependentAlerter 验证组件注册失败会使用已装配的独立告警入口。
func TestComponentRegisterFailureUsesIndependentAlerter(t *testing.T) {
	alerter := &lifecycleAlertRecorder{}
	svcCtx := &svc.ServiceContext{RuntimeAlerter: alerter}
	notifyComponentRegisterFailure(context.Background(), svcCtx, assertLifecycleError{})
	if len(alerter.alerts) != 1 {
		t.Fatalf("组件注册失败告警数量=%d, want 1", len(alerter.alerts))
	}
	alert := alerter.alerts[0]
	if alert.Operation != "register" || alert.TaskName != "component_registry" {
		t.Fatalf("组件注册失败告警不符合预期: %+v", alert)
	}
}

// TestAppStopWaitsForRequestBackgroundTask 校验应用停机先等待请求派生任务，再释放共享资源并返回。
func TestAppStopWaitsForRequestBackgroundTask(t *testing.T) {
	svcCtx := svc.NewServiceContext(config.Config{}, svc.Dependencies{})
	started := make(chan struct{})
	release := make(chan struct{})
	if !svcCtx.GoBackground(func() {
		close(started)
		<-release
	}) {
		t.Fatal("GoBackground() rejected task before shutdown")
	}
	<-started

	stopResult := make(chan error, 1)
	go func() {
		stopResult <- (&App{ServiceContext: svcCtx}).Stop(context.Background())
	}()
	select {
	case err := <-stopResult:
		t.Fatalf("App.Stop() returned before background task finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-stopResult; err != nil {
		t.Fatalf("App.Stop() error = %v", err)
	}
}

// lifecycleAlertRecorder 记录生命周期测试告警。
type lifecycleAlertRecorder struct {
	alerts []svc.TaskRuntimeAlert // 已接收的运行告警
}

// NotifyRuntimeAlert 记录一次运行告警。
func (r *lifecycleAlertRecorder) NotifyRuntimeAlert(_ context.Context, alert svc.TaskRuntimeAlert) {
	r.alerts = append(r.alerts, alert)
}

// assertLifecycleError 是生命周期测试使用的固定错误。
type assertLifecycleError struct{}

// Error 返回测试错误文本。
func (assertLifecycleError) Error() string {
	return "lifecycle failed"
}
