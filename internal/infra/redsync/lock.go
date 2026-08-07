package redsync

import (
	"context"
	stderrors "errors"
	"math/rand/v2"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/Is999/go-utils/errors"
	redsyncv4 "github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

var (
	// ErrLockLost 表示锁在持有期间已经过期或续期失败，调用方应尽快停止受保护逻辑。
	ErrLockLost = errors.New("Redis 锁已丢失")
	// ErrLockTaken 表示 Redis 锁已被其它任务持有，调用方可按业务语义选择跳过本次触发。
	ErrLockTaken = errors.New("Redis 锁已被占用")
)

const (
	// lockAcquireTries 包含首次立即请求和五次退避重试，持续竞争时最多向 Redis 发起六轮加锁。
	lockAcquireTries = 6
	// lockRetryStep 是相邻重试轮次增加的基础等待，五次基础等待依次为 100/200/300/400/500ms。
	lockRetryStep = 100 * time.Millisecond
	// maxLockRetryBaseDelay 把末轮基础等待限制在 500ms，避免竞争释放后仍长时间空等。
	maxLockRetryBaseDelay = 500 * time.Millisecond
	// maxLockRetryJitter 把同一轮竞争者随机打散到 100ms 窗口，同时把单轮最长等待限制在 600ms。
	maxLockRetryJitter = 100 * time.Millisecond
	// maxLockAcquireTimeout 覆盖最多 2s 纯退避并预留 Redis 请求时间，调用方更短的 deadline 仍优先生效。
	maxLockAcquireTimeout = 3 * time.Second
	// minLockOperationTimeout 是单次 Redis 锁操作的最短超时时间，避免小 TTL 场景下超时过短导致正常网络抖动被误判。
	minLockOperationTimeout = 50 * time.Millisecond
	// maxLockOperationTimeout 是单次 Redis 锁操作的最长超时时间，避免 Redis 异常时续期或释放动作长时间阻塞业务返回。
	maxLockOperationTimeout = 500 * time.Millisecond
)

// Lock 封装基于一个 Redis 逻辑部署的分布式互斥锁，并支持后台自动续期。
// 传入的 UniversalClient 只会适配为一个 redsync pool；Redis Cluster 的分片节点不等同于多套独立 Redis 主节点仲裁。
// 单个 Lock 实例只用于一次加锁/释放生命周期。
type Lock struct {
	rs       *redsyncv4.Redsync // redsync 管理器，负责创建底层分布式互斥锁
	mutex    *redsyncv4.Mutex   // 当前生命周期内持有的底层互斥锁实例
	key      string             // Redis 锁 key，所有实例通过同一个 key 竞争锁
	ttl      time.Duration      // 当前锁生命周期的过期时间，用于计算续期和释放操作的短超时
	cancel   context.CancelFunc // 当前锁生命周期的取消函数，用于加锁失败、续期失败或释放时停止内部上下文
	done     chan struct{}      // 通知续期 goroutine 停止的信号通道
	lost     chan error         // 锁丢失通知通道；续期失败或锁过期时写入具体原因
	opMu     sync.Mutex         // 串行化底层 Extend/Unlock 操作，避免续期和释放并发访问 mutex
	once     sync.Once          // 确保 done 只关闭一次，避免重复 Unlock 触发 panic
	lostOnce sync.Once          // 确保 lost 只关闭一次，避免续期失败和 Unlock 并发关闭
}

// NewLock 基于传入的 Redis 客户端创建分布式锁实例。
func NewLock(redisClient redis.UniversalClient, key string) *Lock {
	// lock 保存一次分布式锁生命周期所需的固定配置和内部状态。
	lock := &Lock{
		key:  strings.TrimSpace(key),
		done: make(chan struct{}),
		lost: make(chan error, 1),
	}
	if redisClient != nil {
		// pool 将 go-redis 客户端适配为 redsync 可使用的 Redis 连接池。
		pool := goredis.NewPool(redisClient)
		lock.rs = redsyncv4.New(pool)
	}
	return lock
}

// TryLock 按全局有界退避策略获取锁；加锁成功后会自动续期，直到调用 Unlock 或续期失败。
func (l *Lock) TryLock(ctx context.Context, ttl time.Duration) error {
	return l.tryLock(ctx, ttl, false)
}

// tryLock 根据 singleAttempt 选择单次抢锁或有界退避，成功后的续期与释放生命周期保持一致。
func (l *Lock) tryLock(ctx context.Context, ttl time.Duration, singleAttempt bool) error {
	// 基础参数先校验，避免 nil Redis 客户端或非法 TTL 在 redsync 内部触发 panic 或异常行为。
	if l == nil || l.rs == nil {
		return errors.Errorf("Redis 锁未初始化")
	}
	if l.key == "" {
		return errors.Errorf("Redis 锁 key 不能为空")
	}
	if ttl <= 0 {
		return errors.Errorf("Redis 锁 TTL 必须大于 0")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// lockCtx 只控制本次加锁动作和续期生命周期，不反向取消调用方传入的父 context。
	lockCtx, cancel := context.WithCancel(ctx)
	l.ttl = ttl
	l.cancel = cancel

	// mutex 是当前锁生命周期真正执行 Redis SETNX/PEXPIRE/DEL 等操作的底层互斥锁。
	l.mutex = l.rs.NewMutex(l.key,
		redsyncv4.WithExpiry(ttl),
		redsyncv4.WithTries(lockAcquireTries),
		redsyncv4.WithRetryDelayFunc(func(tries int) time.Duration {
			// jitter 使用进程级并发安全随机源，让同一时刻失败的多个竞争者分散重试。
			jitter := time.Duration(rand.Int64N(int64(maxLockRetryJitter) + 1))
			return lockRetryDelay(tries, jitter)
		}),
	)

	// acquireTimeout 限制本轮抢锁；单次模式只预留一个 Redis 操作窗口，有界退避模式覆盖全部五次等待。
	acquireTimeout := maxLockAcquireTimeout
	if singleAttempt {
		acquireTimeout = lockOperationTimeout(ttl)
	}
	acquireCtx, acquireCancel := context.WithTimeout(lockCtx, acquireTimeout)
	var lockErr error
	if singleAttempt {
		// TryLockContext 只执行一轮 SETNX，供冲突即跳过或转为观察结果的调用链使用。
		lockErr = l.mutex.TryLockContext(acquireCtx)
	} else {
		lockErr = l.mutex.LockContext(acquireCtx)
	}
	acquireErr := acquireCtx.Err()
	acquireCancel()
	if lockErr != nil {
		cancel()
		if acquireErr != nil {
			return errors.Wrap(acquireErr, "获取 Redis 锁失败")
		}
		if isLockTakenError(lockErr) {
			return stderrors.Join(errors.Wrap(lockErr, "获取 Redis 锁失败"), ErrLockTaken)
		}
		return errors.Wrap(lockErr, "获取 Redis 锁失败")
	}

	// 加锁成功后再启动续期，避免未持锁时出现无意义的续期 goroutine。
	go l.startRenewal(ttl)
	return nil
}

// lockRetryDelay 计算指定重试轮次的线性等待；基础值达到 500ms 后不再增长，抖动被限制在 0–100ms。
func lockRetryDelay(tries int, jitter time.Duration) time.Duration {
	if tries < 1 {
		tries = 1
	}
	base := time.Duration(tries) * lockRetryStep
	if base > maxLockRetryBaseDelay {
		base = maxLockRetryBaseDelay
	}
	if jitter < 0 {
		jitter = 0
	} else if jitter > maxLockRetryJitter {
		jitter = maxLockRetryJitter
	}
	return base + jitter
}

// IsLockTaken 判断错误是否表示锁被其它节点持有。
// 识别 go-redsync 锁竞争错误，供周期任务降级为跳过。
func IsLockTaken(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrLockTaken) || isLockTakenError(err)
}

// isLockTakenError 通过 go-redsync 稳定错误类型识别锁竞争。
func isLockTakenError(err error) bool {
	if err == nil {
		return false
	}
	var takenErr *redsyncv4.ErrTaken
	return stderrors.As(err, &takenErr)
}

// startRenewal 周期性续期当前锁；一旦续期失败，会通知锁丢失并停止续期。
func (l *Lock) startRenewal(ttl time.Duration) {
	// interval 使用 TTL 的一半，尽量在锁过期前留出一次续期失败的缓冲窗口。
	interval := ttl / 2
	if interval <= 0 {
		interval = ttl
	}
	// ticker 驱动后台续期循环；函数退出时必须停止，避免定时器资源泄漏。
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 续期和释放都要访问底层 mutex，这里串行化可以避免两个 Redis 命令交叉执行。
			l.opMu.Lock()
			select {
			case <-l.done:
				l.opMu.Unlock()
				return
			default:
			}
			if l.mutex == nil || !l.mutex.Until().After(time.Now()) {
				// 先通知锁丢失，再释放 opMu，避免并发 Unlock 抢先把 lost 通道按正常释放关闭。
				l.reportLoss(ErrLockLost)
				l.opMu.Unlock()
				return
			}
			// 单次续期使用短超时；Redis 不可用时必须尽快暴露锁丢失，不能让业务继续误以为仍持有锁。
			renewCtx, cancel := context.WithTimeout(context.Background(), lockOperationTimeout(ttl))
			ok, err := l.mutex.ExtendContext(renewCtx)
			cancel()
			if !ok || err != nil {
				// 续期失败属于锁安全边界被打破，必须优先通知业务层取消受保护逻辑。
				if err != nil {
					l.reportLoss(errors.Wrap(err, "续期 Redis 锁失败"))
				} else {
					l.reportLoss(ErrLockLost)
				}
				l.opMu.Unlock()
				return
			}
			l.opMu.Unlock()
		case <-l.done:
			return
		}
	}
}

// Unlock 停止自动续期并释放分布式锁。
func (l *Lock) Unlock() error {
	if l == nil {
		return nil
	}
	// once 确保停止信号和内部 context 只触发一次，避免重复关闭通道触发 panic。
	l.once.Do(func() {
		if l.done != nil {
			close(l.done)
		}
		if l.cancel != nil {
			l.cancel()
		}
	})
	defer l.closeLost(nil)

	if l.mutex == nil {
		return nil
	}
	// 释放和续期互斥执行，保证不会出现一边续期一边释放导致状态判断混乱。
	l.opMu.Lock()
	defer l.opMu.Unlock()
	// 释放锁同样使用短超时；失败会原样返回给调用方，便于排查锁残留到 TTL 的问题。
	unlockCtx, cancel := context.WithTimeout(context.Background(), lockOperationTimeout(l.ttl))
	defer cancel()
	if ok, err := l.mutex.UnlockContext(unlockCtx); !ok || err != nil {
		if err != nil {
			return errors.Wrap(err, "释放 Redis 锁失败")
		}
		return errors.Errorf("释放 Redis 锁失败")
	}
	return nil
}

// lockOperationTimeout 根据锁 TTL 计算单次 Redis 锁操作的保护超时。
func lockOperationTimeout(ttl time.Duration) time.Duration {
	// 默认按 TTL 的四分之一等待 Redis 响应，让续期失败能在下一轮之前尽快反馈给业务层。
	timeout := ttl / 4
	if timeout < minLockOperationTimeout {
		// 小 TTL 场景下给 Redis 至少 50ms 响应时间，避免过度敏感。
		return minLockOperationTimeout
	}
	if timeout > maxLockOperationTimeout {
		// 大 TTL 场景下也不让单次 Redis 锁操作超过 500ms，避免阻塞调用链。
		return maxLockOperationTimeout
	}
	return timeout
}

// Lost 返回锁丢失通知通道；正常释放时通道会关闭且不会返回错误。
func (l *Lock) Lost() <-chan error {
	if l == nil || l.lost == nil {
		closed := make(chan error)
		close(closed)
		return closed
	}
	return l.lost
}

// reportLoss 记录锁丢失原因，并取消当前锁生命周期上下文。
func (l *Lock) reportLoss(err error) {
	if err == nil {
		err = ErrLockLost
	} else if !errors.Is(err, ErrLockLost) {
		// 丢锁哨兵和真实原因同时保留，调用方既可稳定分类，也能看到 Redis 原始故障。
		err = stderrors.Join(ErrLockLost, err)
	}
	if l.cancel != nil {
		l.cancel()
	}
	l.closeLost(err)
}

// closeLost 关闭锁丢失通知通道；err 非空时会在关闭前发送给监听方。
func (l *Lock) closeLost(err error) {
	if l == nil || l.lost == nil {
		return
	}
	l.lostOnce.Do(func() {
		if err != nil {
			l.lost <- err
		}
		close(l.lost)
	})
}

// drainError 非阻塞读取错误通道，避免 WithLock 在收尾阶段因为监听协程阻塞。
func drainError(ch <-chan error) error {
	select {
	case err, ok := <-ch:
		if ok {
			return errors.Tag(err)
		}
	default:
	}
	return nil
}

// WithLock 使用全局有界退避策略获取 Redis 锁，并在持锁期间执行 fn。
func WithLock(ctx context.Context, redisClient redis.UniversalClient, key string, ttl time.Duration, fn func(context.Context) error) (err error) {
	return withLock(ctx, redisClient, key, ttl, false, fn)
}

// WithLockOnce 只抢锁一次；锁已被占用时在单次抢锁轮次结束后报告 ErrLockTaken，不执行退避重试或 fn。
// 该入口只用于冲突可直接跳过，或调用方会另行观察已有结果的链路；必须等待临界区的业务继续使用 WithLock。
func WithLockOnce(ctx context.Context, redisClient redis.UniversalClient, key string, ttl time.Duration, fn func(context.Context) error) (err error) {
	return withLock(ctx, redisClient, key, ttl, true, fn)
}

// withLock 复用抢锁后的续期、丢锁通知和 owner 校验释放流程，singleAttempt 只改变获取阶段。
func withLock(ctx context.Context, redisClient redis.UniversalClient, key string, ttl time.Duration, singleAttempt bool, fn func(context.Context) error) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// runCtx 是传给业务函数的派生 context；锁续期失败时会主动取消它。
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	// 先获取锁，获取失败时不执行 fn，避免业务逻辑在无锁保护下运行。
	lock := NewLock(redisClient, key)
	if err := lock.tryLock(runCtx, ttl, singleAttempt); err != nil {
		return errors.Tag(err)
	}

	// 监听续期失败；一旦锁丢失，取消传给 fn 的 runCtx，让业务逻辑尽快退出。
	// renewalErrCh 保存续期失败原因，最后与业务错误、释放错误一起返回。
	renewalErrCh := make(chan error, 1)
	// watcherDone 用于等待监听协程退出，避免 WithLock 返回时留下 goroutine。
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		lostErr, ok := <-lock.Lost()
		if ok && lostErr != nil {
			runCancel()
			renewalErrCh <- lostErr
		}
	}()

	// 统一收尾保证正常返回、业务错误和回调异常都会停止续期并释放锁。
	// 回调异常在锁边界转换为 error，避免未挂 Recover 的后台调用链终止服务进程。
	defer func() {
		if recovered := recover(); recovered != nil {
			err = stderrors.Join(err, errors.Errorf("Redis 锁业务回调异常 key=%s value=%v stack=%s", key, recovered, debug.Stack()))
		}
		if unlockErr := lock.Unlock(); unlockErr != nil {
			err = stderrors.Join(err, unlockErr)
		}
		<-watcherDone
		if lostErr := drainError(renewalErrCh); lostErr != nil {
			err = stderrors.Join(err, lostErr)
		}
		err = errors.Tag(err)
	}()

	if fn != nil {
		// fn 接收 runCtx；如果续期失败，runCtx 会被取消，业务逻辑可以据此尽快退出。
		err = fn(runCtx)
	}
	return err
}
