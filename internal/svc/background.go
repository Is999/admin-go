package svc

import (
	"context"
	"runtime/debug"
	"sync"

	"admin/internal/infra/loggerx"

	"github.com/Is999/go-utils/errors"
	"github.com/zeromicro/go-zero/core/logx"
)

// backgroundTasks 管理请求完成后仍需继续执行的短后台任务。
// Stop 关闭接收入口并等待已登记任务，避免数据库和日志资源先于任务释放。
type backgroundTasks struct {
	mu        sync.Mutex     // 保护 accepting 与 WaitGroup.Add，避免 Add 和 Stop 并发穿透
	accepting bool           // false 表示停机已开始，不再接收新的后台任务
	wg        sync.WaitGroup // 记录已经登记且尚未结束的短后台任务
}

// newBackgroundTasks 创建处于接收状态的进程级短后台任务集合。
func newBackgroundTasks() *backgroundTasks {
	return &backgroundTasks{accepting: true}
}

// Go 登记并异步执行任务；停机开始后返回 false，调用方不得再自行启动游离 goroutine。
func (g *backgroundTasks) Go(task func()) bool {
	if g == nil || task == nil {
		return false
	}
	g.mu.Lock()
	if !g.accepting {
		g.mu.Unlock()
		return false
	}
	g.wg.Add(1)
	g.mu.Unlock()

	go func() {
		defer g.wg.Done()
		defer func() {
			if recovered := recover(); recovered != nil {
				loggerx.Errorw(context.Background(), "短后台任务发生异常", errors.Errorf("panic=%v", recovered),
					logx.Field("stacktrace", string(debug.Stack())),
				)
			}
		}()
		task()
	}()
	return true
}

// Stop 拒绝新任务并等待已登记任务完成；ctx 到期时返回受控错误，不阻塞其它资源关闭。
func (g *backgroundTasks) Stop(ctx context.Context) error {
	if g == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g.mu.Lock()
	g.accepting = false
	g.mu.Unlock()

	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "等待短后台任务停止失败")
	}
}
