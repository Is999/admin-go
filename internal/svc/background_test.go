package svc

import (
	"context"
	"testing"
	"time"
)

// TestBackgroundTasksStopWaitsAndRejectsNewTasks 校验停机先等待已登记任务，并拒绝停机后的新任务。
func TestBackgroundTasksStopWaitsAndRejectsNewTasks(t *testing.T) {
	group := newBackgroundTasks()
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	if !group.Go(func() {
		close(started)
		<-release
		close(finished)
	}) {
		t.Fatal("Go() rejected task before shutdown")
	}
	<-started

	stopResult := make(chan error, 1)
	go func() {
		stopResult <- group.Stop(context.Background())
	}()
	select {
	case err := <-stopResult:
		t.Fatalf("Stop() returned before registered task finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-stopResult; err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	<-finished
	if group.Go(func() {}) {
		t.Fatal("Go() accepted task after shutdown")
	}
}

// TestBackgroundTasksStopHonorsContext 校验任务未结束时 Stop 按调用方期限返回，不造成无界停机等待。
func TestBackgroundTasksStopHonorsContext(t *testing.T) {
	group := newBackgroundTasks()
	release := make(chan struct{})
	if !group.Go(func() { <-release }) {
		t.Fatal("Go() rejected task before shutdown")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := group.Stop(ctx); err == nil {
		t.Fatal("Stop() error = nil, want context deadline error")
	}
	close(release)
}
