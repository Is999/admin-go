package collectorx

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"admin/internal/config"
	"admin/internal/infra/loggerx"
	"admin/internal/model"

	"github.com/Is999/go-utils/errors"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// Collector 默认参数统一控制批次、重试和超时行为。
const (
	defaultFailureRetryBatchSize        = 500                   // 失败账本单轮领取的默认事件数量
	defaultFailureRetryIntervalSeconds  = 1                     // 失败账本空轮询后的默认等待秒数
	defaultFailureRunningLeaseSeconds   = 600                   // 失败账本运行中事件的默认租约秒数
	maxFailureRetryIdleInterval         = 5 * time.Second       // 失败账本空闲轮询最大等待时间
	minFailureRecoveryInterval          = 30 * time.Second      // 失败账本运行租约最小回收检查间隔
	maxFailureRecoveryInterval          = 60 * time.Second      // 失败账本运行租约最大回收检查间隔
	defaultFailureMaxRetryTimes         = 8                     // 失败事件默认最大重试次数
	defaultCollectorBatchSize           = 500                   // Collector 默认本地批量条数
	defaultCollectorBatchWait           = 20 * time.Millisecond // Collector 默认本地聚合等待时长
	defaultProcessorTimeout             = 5 * time.Minute       // Processor 默认单批执行超时
	defaultKafkaWriteTimeout            = 10 * time.Second      // Kafka 默认写入超时时间
	defaultIdempotencyPipelineBatchSize = 200                   // Collector Redis 幂等 Pipeline 默认命令数
)

// Collector Redis 默认租约与终态 TTL 复用配置层唯一默认值。
const (
	defaultIdempotencyTTL           = time.Duration(config.DefaultCollectorIdempotencyTTLSeconds) * time.Second           // Collector Redis 幂等终态保留时间
	defaultIdempotencyProcessingTTL = time.Duration(config.DefaultCollectorIdempotencyProcessingTTLSeconds) * time.Second // Collector Redis 处理中占用租约时间
)

// Collector 运行上限防止错误配置放大批量、等待和 Redis 压力。
const (
	maxCollectorTaskBatchWait       = time.Minute            // 单个 Collector 任务最大聚合等待时间
	maxCollectorProcessorTimeout    = time.Hour              // 单个 Processor 执行超时硬上限
	maxCollectorCarrierBatchSize    = 5000                   // Collector 单批载体处理数量上限
	maxIdempotencyPipelineBatchSize = 1000                   // Collector Redis 幂等 Pipeline 命令数上限
	slowCollectorBatchThreshold     = 200 * time.Millisecond // Collector 批量处理慢日志阈值
)

// Collector 事件上限与失败账本字段及 Kafka 信封契约保持一致。
const (
	maxCollectorEventIDBytes      = 64        // 事件 ID 字节上限
	maxCollectorBizTypeBytes      = 100       // 业务类型字节上限
	maxCollectorPartitionKeyBytes = 128       // 分区键字节上限
	maxCollectorPayloadBytes      = 60 * 1024 // 事件 JSON 负载上限，为 MySQL TEXT 保留安全余量
	maxCollectorKafkaMessageBytes = 64 * 1024 // Kafka 事件信封上限，兼作常规 fetch 目标
	maxCollectorLastErrorBytes    = 1000      // 失败原因字节上限
	maxInvalidKafkaTopicBytes     = 249       // Kafka Topic 诊断快照上限
	maxInvalidKafkaKeyBytes       = 1024      // 坏消息 Kafka key 诊断快照上限
	maxInvalidKafkaRawBytes       = 4096      // 坏消息原始内容诊断快照上限
)

// kafkaMessageWriter 约束 Kafka 写入器，便于单测替换真实 broker。
type kafkaMessageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

// Manager 负责通用收集器的 Kafka 投递、批量消费和失败账本重试。
type Manager struct {
	cfg          config.CollectorConfig        // 收集器运行配置
	failures     *gorm.DB                      // 失败账本连接，仅保存失败、死信和人工重试记录
	writers      map[string]kafkaMessageWriter // Kafka Topic 到写入器的映射
	runtimeStats *collectorRuntimeStats        // 当前进程运行态指标，仅保存聚合快照
	idempotency  *idempotencyStore             // Redis 单任务 EventID 幂等去重存储

	mu         sync.RWMutex         // 保护 processors 注册表
	processors map[string]Processor // bizType 到批量处理器的映射
	alertHook  AlertHook            // 后台运行异常告警钩子

	ctx             context.Context    // 后台 worker 生命周期上下文
	cancel          context.CancelFunc // 后台 worker 停止函数
	wg              sync.WaitGroup     // 等待后台 worker 退出
	running         atomic.Bool        // Collector 后台 worker 是否已启动
	activeWorkers   atomic.Int32       // 当前实际运行的消费与失败重试 worker 数量
	expectedWorkers atomic.Int32       // 当前配置要求运行的 worker 数量

	kafkaReaders []*kafka.Reader // Kafka 消费器列表，每个任务 Topic 一个 Reader
}

// eventKey 唯一标识 Collector 业务类型内的一条事件。
type eventKey struct {
	bizType string // 业务类型
	eventID string // 事件 ID
}

// eventKeyOf 返回事件的业务复合键。
func eventKeyOf(event Event) eventKey {
	return eventKey{bizType: event.BizType, eventID: event.EventID}
}

// kafkaBatch 表示一批 Kafka 消息及其解析后的有效事件和坏消息失败记录。
type kafkaBatch struct {
	messages []kafka.Message    // 本批需要统一提交的 Kafka 消息
	events   []Event            // 可进入 Processor 的有效事件
	invalid  []failureEventSeed // 不可恢复的坏消息，落失败账本后提交 offset
}

// kafkaWorkItem 表示 Kafka partition lane 内按 offset 排队的单条消息。
type kafkaWorkItem struct {
	message kafka.Message     // 原始 Kafka 消息，用于成功后提交 offset
	event   Event             // 解析成功的业务事件
	failure *failureEventSeed // 解析失败时写入失败账本的死信记录
}

// kafkaCommitFunc 串行提交 Kafka offset，避免多个 partition lane 并发调用 reader。
type kafkaCommitFunc func(context.Context, ...kafka.Message) error

// eventTaskGroup 表示按 bizType 聚合后的稳定任务批次。
type eventTaskGroup struct {
	bizType string  // 业务类型
	events  []Event // 同一业务类型下待处理事件
}

// collectorBatchPolicy 表示单个 bizType 在 partition lane 内的聚合触发策略。
type collectorBatchPolicy struct {
	batchSize int           // 同任务最大聚合条数
	batchWait time.Duration // 同任务未满批时最大等待时间
}

// collectorKafkaRoute 表示单个业务任务的 Kafka 投递和消费路由。
type collectorKafkaRoute struct {
	topic   string // Kafka 主题
	groupID string // Kafka 消费组
}

// failureEventSeed 表示待写入失败账本的事件快照。
type failureEventSeed struct {
	event     Event                           // 结构化事件；坏消息使用框架生成的合成事件
	state     model.CollectorFailedEventState // 写入失败账本的初始状态
	attempt   int                             // 已失败次数
	nextRunAt time.Time                       // 下次允许重试时间
	lastError string                          // 失败原因摘要
}

// invalidKafkaSnapshot 保存坏消息的有界诊断信息，内容必须能安全写入失败账本 TEXT 字段。
type invalidKafkaSnapshot struct {
	Topic              string `json:"topic"`              // Topic 诊断快照
	Partition          int    `json:"partition"`          // Kafka 分区号
	Offset             int64  `json:"offset"`             // Kafka 消费位点
	Key                string `json:"key"`                // 消息 key 有界快照
	Raw                string `json:"raw"`                // 原始消息有界快照
	OriginalKeyBytes   int    `json:"originalKeyBytes"`   // 原始 key 字节数
	OriginalValueBytes int    `json:"originalValueBytes"` // 原始消息体字节数
}

// collectorDeadSummary 表示一次回写中进入死信的事件摘要。
type collectorDeadSummary struct {
	count   int    // 进入死信的事件数量
	bizType string // 首个死信事件业务类型
	eventID string // 首个死信事件 ID
}

// New 创建通用收集器管理器，并按配置初始化 Kafka 写入器。
func New(cfg config.CollectorConfig, failureDB *gorm.DB, redisClient redis.UniversalClient) (*Manager, error) {
	if failureDB == nil {
		return nil, errors.Errorf("collector failureDB 为空")
	}
	if cfg.Enabled && collectorAnyIdempotencyEnabled(cfg) && redisClient == nil {
		return nil, errors.Errorf("collector.enabled=true 时必须提供 Redis 幂等存储")
	}
	if err := RegisterMetrics(); err != nil {
		return nil, errors.Wrap(err, "注册 Collector 指标失败")
	}
	normalizeCollectorTaskRoutes(&cfg)

	m := &Manager{
		cfg:          cfg,
		failures:     failureDB,
		writers:      make(map[string]kafkaMessageWriter),
		runtimeStats: newCollectorRuntimeStats(),
		idempotency: newIdempotencyStore(
			redisClient,
			collectorIdempotencyTTL(cfg.Idempotency),
			collectorIdempotencyProcessingTTL(cfg.Idempotency),
			collectorIdempotencyPipelineBatchSize(cfg.Idempotency),
		),
		processors: make(map[string]Processor),
	}

	if cfg.Enabled && len(cfg.Kafka.Brokers) > 0 {
		timeout := time.Duration(cfg.Kafka.WriteTimeout) * time.Second
		if timeout <= 0 {
			timeout = defaultKafkaWriteTimeout
		}
		batchSize := boundedPositiveInt(cfg.Kafka.WriteBatchSize, defaultCollectorBatchSize, maxCollectorCarrierBatchSize)
		for _, topic := range collectorConfiguredTopics(cfg) {
			m.writers[topic] = &kafka.Writer{
				Addr:         kafka.TCP(cfg.Kafka.Brokers...),
				Topic:        topic,
				Balancer:     &kafka.Hash{},
				BatchSize:    batchSize,
				BatchTimeout: m.kafkaWriteBatchWait(),
				WriteTimeout: timeout,
				RequiredAcks: kafka.RequireAll,
				Async:        false,
			}
		}
	}

	return m, nil
}

// SetAlertHook 设置 Collector 后台运行异常告警钩子，可接入 Lark 等外部告警系统。
func (m *Manager) SetAlertHook(hook AlertHook) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alertHook = hook
}

// RegisterProcessor 注册指定 bizType 的批量消费处理器。
func (m *Manager) RegisterProcessor(bizType string, p Processor) error {
	if m == nil {
		return errors.Errorf("collector 未初始化")
	}
	bizType = strings.TrimSpace(bizType)
	if bizType == "" {
		return errors.Errorf("collectorx.RegisterProcessor bizType 为空")
	}
	if p == nil {
		return errors.Errorf("collectorx.RegisterProcessor processor 为空")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.processors[bizType]; ok {
		return errors.Errorf("collectorx.RegisterProcessor 重复注册 bizType=%s", bizType)
	}
	m.processors[bizType] = p
	allowMetricBizTypeLabel(bizType)
	return nil
}

// RegisterProcessorFunc 允许业务方直接传入批量消费函数，避免额外定义结构体。
func (m *Manager) RegisterProcessorFunc(bizType string, fn ProcessorFunc) error {
	if fn == nil {
		return errors.Errorf("collectorx.RegisterProcessorFunc processor 为空")
	}
	return errors.Tag(m.RegisterProcessor(bizType, fn))
}

// Start 启动通用收集器后台 worker。
func (m *Manager) Start() {
	if m == nil || !m.cfg.Enabled {
		return
	}
	if !m.running.CompareAndSwap(false, true) {
		return
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	routes := collectorConfiguredReadRoutes(m.cfg)
	m.expectedWorkers.Store(int32(1 + len(routes)))

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.activeWorkers.Add(1)
		defer m.activeWorkers.Add(-1)
		m.runFailureRetryWorker()
	}()

	if len(m.cfg.Kafka.Brokers) > 0 {
		for _, route := range routes {
			reader := kafka.NewReader(m.kafkaReaderConfig(route))
			m.kafkaReaders = append(m.kafkaReaders, reader)
			topic := route.topic
			groupID := route.groupID
			m.wg.Add(1)
			go func() {
				defer m.wg.Done()
				m.activeWorkers.Add(1)
				defer m.activeWorkers.Add(-1)
				m.runKafkaWorker(reader, topic, groupID)
			}()
		}
	}
}

// kafkaReaderConfig 返回有界的常规 fetch 目标；超限单消息仍由消费后校验转入死信。
func (m *Manager) kafkaReaderConfig(route collectorKafkaRoute) kafka.ReaderConfig {
	return kafka.ReaderConfig{
		Brokers:  m.cfg.Kafka.Brokers,
		GroupID:  route.groupID,
		Topic:    route.topic,
		MaxBytes: maxCollectorKafkaMessageBytes,
	}
}

// Stop 停止通用收集器后台 worker，并关闭 Kafka 读写资源。
func (m *Manager) Stop(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	m.running.Store(false)
	if m.cancel != nil {
		m.cancel()
	}
	var firstErr error
	recordErr := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = errors.Tag(err)
		}
	}
	readerClosers := make([]func() error, 0, len(m.kafkaReaders))
	for _, reader := range m.kafkaReaders {
		readerClosers = append(readerClosers, reader.Close)
	}
	recordErr(closeAllWithContext(ctx, readerClosers))
	if ctx.Err() != nil {
		return errors.Tag(firstErr)
	}
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		recordErr(errors.Wrap(ctx.Err(), "等待 Collector 后台协程退出超时"))
		return errors.Tag(firstErr)
	}
	writerClosers := make([]func() error, 0, len(m.writers))
	for _, topic := range sortedWriterTopics(m.writers) {
		writerClosers = append(writerClosers, m.writers[topic].Close)
	}
	recordErr(closeAllWithContext(ctx, writerClosers))
	return errors.Tag(firstErr)
}

// closeAllWithContext 并发关闭彼此独立的 Kafka 资源，并受应用停止期限约束。
func closeAllWithContext(ctx context.Context, closeFns []func() error) error {
	if len(closeFns) == 0 {
		return nil
	}
	errs := make(chan error, len(closeFns))
	var wg sync.WaitGroup
	wg.Add(len(closeFns))
	for _, closeFn := range closeFns {
		go func() {
			defer wg.Done()
			errs <- closeFn()
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		close(errs)
		for err := range errs {
			if err != nil {
				return errors.Tag(err)
			}
		}
		return nil
	case <-ctx.Done():
		return errors.Tag(ctx.Err())
	}
}

// Ready 检查 Kafka 连通性以及 Worker 模式下的后台协程状态。
func (m *Manager) Ready(ctx context.Context, requireWorker bool) error {
	if m == nil || !m.cfg.Enabled {
		return errors.New("Collector未初始化或未启用")
	}
	if len(m.writers) == 0 {
		return errors.New("Collector Kafka写入器未初始化")
	}
	if err := pingKafkaBrokers(ctx, m.cfg.Kafka.Brokers, collectorConfiguredTopics(m.cfg)); err != nil {
		return errors.Tag(err)
	}
	if !requireWorker {
		return nil
	}
	if !m.running.Load() {
		return errors.New("Collector后台Worker未运行")
	}
	expected := m.expectedWorkers.Load()
	if active := m.activeWorkers.Load(); active < expected {
		return errors.Errorf("Collector后台Worker不完整 active=%d expected=%d", active, expected)
	}
	return nil
}

// pingKafkaBrokers 通过 Kafka 元数据协议检查所有 Collector Topic。
func pingKafkaBrokers(ctx context.Context, brokers []string, topics []string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var lastErr error
	dialer := &kafka.Dialer{Timeout: time.Second}
	for _, broker := range brokers {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			continue
		}
		brokerReady := true
		for _, topic := range topics {
			partitions, err := dialer.LookupPartitions(ctx, "tcp", broker, topic)
			if err == nil && len(partitions) > 0 {
				continue
			}
			if err == nil {
				err = errors.Errorf("Collector Kafka Topic不存在或无分区 topic=%s", topic)
			}
			lastErr = err
			brokerReady = false
			break
		}
		if brokerReady && len(topics) > 0 {
			return nil
		}
		if ctx.Err() != nil {
			break
		}
	}
	if lastErr == nil {
		return errors.New("Collector Kafka broker未配置")
	}
	return errors.Wrap(lastErr, "Collector Kafka broker或Topic不可用")
}

// RunNow 手动执行一轮失败账本重试，便于管理端立即处理已修复事件。
func (m *Manager) RunNow(ctx context.Context, limit int) (int, error) {
	if m == nil || !m.cfg.Enabled {
		return 0, errors.Errorf("collector 未启用")
	}
	return m.runFailureRetryOnce(ctx, limit)
}

// Enqueue 投递一条结构化业务事件。
// 正常链路只写 Kafka；Kafka 写入失败直接返回错误，由调用方决定是否同步重试或补偿。
func (m *Manager) Enqueue(ctx context.Context, event Event) (string, error) {
	if m == nil || !m.cfg.Enabled {
		return "", errors.Errorf("collector 未启用")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := normalizeAndValidateEvent(&event); err != nil {
		return "", errors.Tag(err)
	}
	route := m.collectorTaskKafkaRoute(event.BizType)

	body, err := json.Marshal(event)
	if err != nil {
		return "", errors.Tag(err)
	}
	if len(body) > maxCollectorKafkaMessageBytes {
		return "", errors.Errorf("collector Kafka 消息不能超过 %d 字节", maxCollectorKafkaMessageBytes)
	}
	if !m.kafkaAvailable(route) {
		err = errors.Errorf("collector kafka topic 未配置 bizType=%s", event.BizType)
		recordKafkaPublish("failed", 1)
		m.recordRuntimeKafkaPublish(event.BizType, false)
		m.reportRuntimeAlert(ctx, RuntimeAlert{
			Kind:      RuntimeAlertKindEnqueueFailed,
			Title:     "【P1 Collector Kafka 投递不可用】",
			Status:    "Kafka 未配置，事件未进入 Collector 正常链路",
			Component: "collector",
			Operation: "enqueue_kafka_unavailable",
			BizType:   event.BizType,
			Channel:   collectorRuntimeChannelKafka,
			UniqueKey: event.BizType,
			Reason:    collectorAlertReason("eventId", event.EventID, err.Error()),
			Advice:    "请检查 collector.kafka.brokers 以及 collector.tasks.<bizType>.topic 或 collector.default_task.topic。",
		})
		return "", err
	}
	if err = m.publishKafka(ctx, route.topic, kafkaMessageKey(event), body); err != nil {
		recordKafkaPublish("failed", 1)
		m.recordRuntimeKafkaPublish(event.BizType, false)
		m.reportRuntimeAlert(ctx, RuntimeAlert{
			Kind:      RuntimeAlertKindEnqueueFailed,
			Title:     "【P1 Collector Kafka 投递失败】",
			Status:    "Kafka 写入失败，事件未进入 Collector 正常链路",
			Component: "collector",
			Operation: "enqueue_kafka_publish",
			BizType:   event.BizType,
			Channel:   collectorRuntimeChannelKafka,
			UniqueKey: route.topic,
			Reason:    collectorAlertReason("eventId", event.EventID, err.Error()),
			Advice:    "请检查 Kafka broker、topic、ACK 和网络状态；调用方应按自身语义重试或补偿。",
		})
		return "", errors.Tag(err)
	}
	recordKafkaPublish("success", 1)
	m.recordRuntimeKafkaPublish(event.BizType, true)
	return event.EventID, nil
}

// kafkaAvailable 判断指定任务 Kafka 路由是否具备写入条件。
func (m *Manager) kafkaAvailable(route collectorKafkaRoute) bool {
	if m == nil || strings.TrimSpace(route.topic) == "" {
		return false
	}
	return m.writers[route.topic] != nil
}

// publishKafka 将事件写入 Kafka，并等待 broker ACK。
func (m *Manager) publishKafka(ctx context.Context, topic string, key string, body []byte) error {
	writer := m.writers[strings.TrimSpace(topic)]
	if writer == nil {
		return errors.Errorf("collector kafka topic 未配置 topic=%s", topic)
	}
	timeout := time.Duration(m.cfg.Kafka.WriteTimeout) * time.Second
	if timeout <= 0 {
		timeout = defaultKafkaWriteTimeout
	}
	writeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return errors.Tag(writer.WriteMessages(writeCtx, kafka.Message{Key: []byte(key), Value: body}))
}

// kafkaMessageKey 返回 Kafka 分区键，保证同任务同业务分区稳定进入同一 Kafka partition。
func kafkaMessageKey(event Event) string {
	bizType := strings.TrimSpace(event.BizType)
	partitionKey := strings.TrimSpace(event.PartitionKey)
	if partitionKey == "" {
		return bizType
	}
	return bizType + ":" + partitionKey
}

// processorFor 根据 bizType 查找已注册的批量处理器。
func (m *Manager) processorFor(bizType string) (Processor, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p := m.processors[bizType]
	return p, p != nil
}

// processBatch 按 bizType 分组调用 Processor，并返回成功和失败事件集合。
func (m *Manager) processBatch(ctx context.Context, events []Event) (map[eventKey]struct{}, map[eventKey]string) {
	success := make(map[eventKey]struct{}, len(events))
	failed := make(map[eventKey]string)
	for _, task := range groupEventsByBizType(events) {
		bizType := task.bizType
		group := task.events
		beginAt := time.Now()
		groupSuccessCount := 0
		groupFailCount := 0
		p, ok := m.processorFor(bizType)
		if !ok {
			for _, event := range group {
				failed[eventKeyOf(event)] = "collector 未注册 processor bizType=" + bizType
			}
			groupFailCount = len(group)
			m.reportRuntimeAlert(ctx, RuntimeAlert{
				Kind:      RuntimeAlertKindWorkerFailed,
				Title:     "【P1 Collector Processor 缺失】",
				Status:    "该业务类型事件无法消费，失败事件会写入失败账本",
				Component: "collector",
				Operation: "process_missing_processor",
				BizType:   bizType,
				Channel:   collectorRuntimeChannelKafka,
				UniqueKey: bizType,
				Reason:    "collector 未注册 processor bizType=" + bizType,
				Count:     len(group),
				Advice:    "请确认当前镜像是否注册了该 bizType 的 Processor，以及 collector.enabled、业务接入配置和 worker 部署版本是否一致。",
			})
			m.recordProcessorBatchMetrics(bizType, len(group), groupSuccessCount, groupFailCount, time.Since(beginAt))
			continue
		}
		results, err := m.processBizBatch(ctx, bizType, p, group)
		if err != nil {
			for _, event := range group {
				failed[eventKeyOf(event)] = err.Error()
			}
			groupFailCount = len(group)
			m.reportRuntimeAlert(ctx, RuntimeAlert{
				Kind:      RuntimeAlertKindWorkerFailed,
				Title:     "【P1 Collector 批量消费失败】",
				Status:    "本批事件会写入失败账本，后续按重试策略处理",
				Component: "collector",
				Operation: "process_batch",
				BizType:   bizType,
				Channel:   collectorRuntimeChannelKafka,
				UniqueKey: bizType,
				Reason:    err.Error(),
				Count:     len(group),
				Advice:    "请检查对应 Processor 的业务依赖、数据格式和批量写入状态；若失败账本持续上涨，需要先暂停上游放量再处理死信。",
			})
			m.recordProcessorBatchMetrics(bizType, len(group), groupSuccessCount, groupFailCount, time.Since(beginAt))
			continue
		}
		if len(results) == 0 {
			for _, event := range group {
				success[eventKeyOf(event)] = struct{}{}
			}
			groupSuccessCount = len(group)
			m.recordProcessorBatchMetrics(bizType, len(group), groupSuccessCount, groupFailCount, time.Since(beginAt))
			continue
		}
		pending := make(map[string]struct{}, len(group))
		for _, event := range group {
			pending[event.EventID] = struct{}{}
		}
		for _, result := range results {
			eventID := strings.TrimSpace(result.EventID)
			if eventID == "" {
				continue
			}
			if _, ok := pending[eventID]; !ok {
				continue
			}
			delete(pending, eventID)
			key := eventKey{bizType: bizType, eventID: eventID}
			if result.Success {
				success[key] = struct{}{}
				delete(failed, key)
				groupSuccessCount++
				continue
			}
			msg := strings.TrimSpace(result.Error)
			if msg == "" {
				msg = "collector processor 返回失败"
			}
			failed[key] = msg
			groupFailCount++
		}
		for eventID := range pending {
			failed[eventKey{bizType: bizType, eventID: eventID}] = "collector processor 未返回事件处理结果"
			groupFailCount++
		}
		if groupFailCount > 0 {
			m.reportRuntimeAlert(ctx, RuntimeAlert{
				Kind:      RuntimeAlertKindWorkerFailed,
				Title:     "【P1 Collector 部分事件处理失败】",
				Status:    "失败事件会写入失败账本，成功事件直接完成",
				Component: "collector",
				Operation: "process_partial_failed",
				BizType:   bizType,
				Channel:   collectorRuntimeChannelKafka,
				UniqueKey: bizType,
				Reason:    "collector processor 返回部分失败或遗漏结果",
				Count:     groupFailCount,
				Advice:    "请检查 Processor 返回结果是否覆盖所有输入事件，并确认失败事件的错误原因；若 retry/dead 数量持续上升，请优先处理该 bizType。",
			})
		}
		m.recordProcessorBatchMetrics(bizType, len(group), groupSuccessCount, groupFailCount, time.Since(beginAt))
	}
	return success, failed
}

// processBizBatch 在超时上下文中同步执行 Processor，避免取消后留下继续产生副作用的孤儿协程。
func (m *Manager) processBizBatch(ctx context.Context, bizType string, p Processor, group []Event) (results []ProcessResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	processorCtx, cancel := context.WithTimeout(ctx, m.processorTimeout(bizType))
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Errorf("collector processor panic bizType=%s panic=%v", bizType, recovered)
		}
	}()
	results, err = p.ProcessBatch(processorCtx, group)
	if err != nil {
		return results, errors.Tag(err)
	}
	if processorErr := processorCtx.Err(); processorErr != nil {
		return nil, errors.Wrap(processorErr, "collector processor 执行超时或取消 bizType="+bizType)
	}
	return results, nil
}

// groupEventsByBizType 按业务类型稳定聚合事件，保证不同 Processor 的批次隔离且调度顺序可预测。
func groupEventsByBizType(events []Event) []eventTaskGroup {
	groups := make([]eventTaskGroup, 0)
	indexByBizType := make(map[string]int)
	for _, event := range events {
		bizType := event.BizType
		if index, ok := indexByBizType[bizType]; ok {
			groups[index].events = append(groups[index].events, event)
			continue
		}
		indexByBizType[bizType] = len(groups)
		groups = append(groups, eventTaskGroup{
			bizType: bizType,
			events:  []Event{event},
		})
	}
	return groups
}

// beginIdempotentEvents 按任务开关占用 BizType+EventID，保留原始事件顺序并跳过已进入终态的重复事件。
func (m *Manager) beginIdempotentEvents(ctx context.Context, events []Event) ([]Event, []idempotencyClaim, int, error) {
	if len(events) == 0 {
		return events, nil, 0, nil
	}
	enabledEvents := make([]Event, 0, len(events))
	for _, event := range events {
		if m.idempotencyEnabledForBizType(event.BizType) {
			enabledEvents = append(enabledEvents, event)
		}
	}
	if len(enabledEvents) == 0 {
		return events, nil, 0, nil
	}
	store := m.idempotency
	if store == nil {
		return nil, nil, 0, errors.Errorf("collector Redis 幂等存储未初始化")
	}
	_, claims, duplicate, err := store.beginBatch(ctx, enabledEvents)
	if err != nil {
		return nil, nil, 0, errors.Tag(err)
	}
	claimed := make(map[eventKey]struct{}, len(claims))
	for _, claim := range claims {
		claimed[eventKey{bizType: claim.BizType, eventID: claim.EventID}] = struct{}{}
	}
	processEvents := make([]Event, 0, len(events)-duplicate)
	appended := make(map[eventKey]struct{}, len(claims))
	for _, event := range events {
		if !m.idempotencyEnabledForBizType(event.BizType) {
			processEvents = append(processEvents, event)
			continue
		}
		key := eventKey{bizType: strings.TrimSpace(event.BizType), eventID: strings.TrimSpace(event.EventID)}
		if _, ok := claimed[key]; !ok {
			continue
		}
		if _, ok := appended[key]; ok {
			continue
		}
		appended[key] = struct{}{}
		processEvents = append(processEvents, event)
	}
	return processEvents, claims, duplicate, nil
}

// renewIdempotentClaims 仅续期当前 Kafka 批次仍持有的 processing token。
func (m *Manager) renewIdempotentClaims(ctx context.Context, claims []idempotencyClaim) error {
	if len(claims) == 0 {
		return nil
	}
	store := m.idempotency
	if store == nil {
		return errors.Errorf("collector Redis 幂等存储未初始化")
	}
	err := store.renew(ctx, claims)
	result := "success"
	if err != nil {
		result = "failed"
	}
	recordLeaseRenew("redis_idempotency", result)
	return errors.Tag(err)
}

// idempotencyClaimsFromSet 按 Processor 结果筛选当前批次的领取 token。
func idempotencyClaimsFromSet(claims []idempotencyClaim, values map[eventKey]struct{}) []idempotencyClaim {
	items := make([]idempotencyClaim, 0, len(values))
	for _, claim := range claims {
		if _, ok := values[eventKey{bizType: claim.BizType, eventID: claim.EventID}]; ok {
			items = append(items, claim)
		}
	}
	return items
}

// idempotencyClaimsFromFailures 按 Processor 失败结果筛选当前批次的领取 token。
func idempotencyClaimsFromFailures(claims []idempotencyClaim, values map[eventKey]string) []idempotencyClaim {
	items := make([]idempotencyClaim, 0, len(values))
	for _, claim := range claims {
		if _, ok := values[eventKey{bizType: claim.BizType, eventID: claim.EventID}]; ok {
			items = append(items, claim)
		}
	}
	return items
}

// idempotencyEnabledForBizType 返回指定 bizType 的 Redis 单任务 EventID 去重开关。
func (m *Manager) idempotencyEnabledForBizType(bizType string) bool {
	if m == nil {
		return false
	}
	bizType = strings.TrimSpace(bizType)
	if task, ok := m.cfg.Tasks[bizType]; ok && task.IdempotencyEnabled != nil {
		return *task.IdempotencyEnabled
	}
	if m.cfg.DefaultTask.IdempotencyEnabled != nil {
		return *m.cfg.DefaultTask.IdempotencyEnabled
	}
	return m.cfg.Idempotency.Enabled
}

// collectorTaskKafkaRoute 返回指定 bizType 继承后的 Kafka 路由。
func (m *Manager) collectorTaskKafkaRoute(bizType string) collectorKafkaRoute {
	if m == nil {
		return collectorKafkaRoute{}
	}
	bizType = strings.TrimSpace(bizType)
	task := config.CollectorTaskConfig{}
	if candidate, ok := m.cfg.Tasks[bizType]; ok {
		task = candidate
	}
	return collectorKafkaRoute{
		topic:   firstNonEmpty(task.Topic, m.cfg.DefaultTask.Topic),
		groupID: firstNonEmpty(task.GroupID, m.cfg.DefaultTask.GroupID),
	}
}

// normalizeEvent 规范化事件关键字段，确保后续幂等、分组和失败账本都有稳定键。
func normalizeEvent(event *Event) {
	if event == nil {
		return
	}
	event.EventID = strings.TrimSpace(event.EventID)
	event.BizType = strings.TrimSpace(event.BizType)
	event.PartitionKey = strings.TrimSpace(event.PartitionKey)
	if len(event.Payload) == 0 {
		event.Payload = json.RawMessage("null")
	}
}

// normalizeAndValidateEvent 按失败账本存储契约校验事件，避免无法落库的数据进入正常链路。
func normalizeAndValidateEvent(event *Event) error {
	normalizeEvent(event)
	if event == nil {
		return errors.Errorf("collector event 为空")
	}
	if event.BizType == "" {
		return errors.Errorf("collector event bizType 为空")
	}
	if event.EventID == "" {
		return errors.Errorf("collector event eventId 为空")
	}
	if !utf8.ValidString(event.EventID) {
		return errors.Errorf("collector event eventId 不是有效 UTF-8")
	}
	if len(event.EventID) > maxCollectorEventIDBytes {
		return errors.Errorf("collector event eventId 不能超过 %d 字节", maxCollectorEventIDBytes)
	}
	if !utf8.ValidString(event.BizType) {
		return errors.Errorf("collector event bizType 不是有效 UTF-8")
	}
	if len(event.BizType) > maxCollectorBizTypeBytes {
		return errors.Errorf("collector event bizType 不能超过 %d 字节", maxCollectorBizTypeBytes)
	}
	if !utf8.ValidString(event.PartitionKey) {
		return errors.Errorf("collector event partitionKey 不是有效 UTF-8")
	}
	if len(event.PartitionKey) > maxCollectorPartitionKeyBytes {
		return errors.Errorf("collector event partitionKey 不能超过 %d 字节", maxCollectorPartitionKeyBytes)
	}
	if len(event.Payload) > maxCollectorPayloadBytes {
		return errors.Errorf("collector event payload 不能超过 %d 字节", maxCollectorPayloadBytes)
	}
	if !utf8.Valid(event.Payload) {
		return errors.Errorf("collector event payload 不是有效 UTF-8")
	}
	if !json.Valid(event.Payload) {
		return errors.Errorf("collector event payload 不是有效 JSON")
	}
	return nil
}

// runKafkaWorker 从指定 Kafka Topic 拉取事件，并按 partition 分发到独立顺序 lane。
// kafkaFetchBatchSize 返回 partition lane 本地缓冲大小，未配置时使用安全默认值。
func (m *Manager) kafkaFetchBatchSize() int {
	if m == nil {
		return defaultCollectorBatchSize
	}
	size := m.cfg.DefaultTask.BatchSize
	if size <= 0 {
		size = m.cfg.Kafka.WriteBatchSize
	}
	return boundedPositiveInt(size, defaultCollectorBatchSize, maxCollectorCarrierBatchSize)
}

// kafkaWriteBatchWait 返回 Kafka producer 单批写入等待时间。
func (m *Manager) kafkaWriteBatchWait() time.Duration {
	if m == nil || m.cfg.Kafka.WriteBatchWaitMilliseconds <= 0 {
		return defaultCollectorBatchWait
	}
	return time.Duration(boundedPositiveInt(m.cfg.Kafka.WriteBatchWaitMilliseconds, int(defaultCollectorBatchWait/time.Millisecond), 5000)) * time.Millisecond
}

// kafkaReadTimeout 返回 Kafka 单次读取超时。
func (m *Manager) kafkaReadTimeout() time.Duration {
	if m == nil || m.cfg.Kafka.ReadTimeout <= 0 {
		return time.Second
	}
	return time.Duration(m.cfg.Kafka.ReadTimeout) * time.Second
}

// failureRetryBatchSize 返回失败账本 worker 单轮消费批次大小。
func (m *Manager) failureRetryBatchSize() int {
	if m == nil {
		return defaultFailureRetryBatchSize
	}
	return boundedPositiveInt(m.cfg.FailureRetry.RunnerBatchSize, defaultFailureRetryBatchSize, maxCollectorCarrierBatchSize)
}

// failureWriteBatchSize 返回失败账本批量写入大小。
func (m *Manager) failureWriteBatchSize() int {
	size := defaultCollectorBatchSize
	if m != nil {
		if m.cfg.DefaultTask.BatchSize > size {
			size = m.cfg.DefaultTask.BatchSize
		}
		if m.cfg.FailureRetry.RunnerBatchSize > size {
			size = m.cfg.FailureRetry.RunnerBatchSize
		}
	}
	return boundedPositiveInt(size, defaultCollectorBatchSize, maxCollectorCarrierBatchSize)
}

// failureMaxRetryTimes 返回失败账本最大重试次数。
func (m *Manager) failureMaxRetryTimes() int {
	if m == nil || m.cfg.FailureRetry.MaxRetryTimes <= 0 {
		return defaultFailureMaxRetryTimes
	}
	return m.cfg.FailureRetry.MaxRetryTimes
}

// failureRunningLease 返回失败账本领取租约。
func (m *Manager) failureRunningLease() time.Duration {
	if m == nil || m.cfg.FailureRetry.RunningLeaseSeconds <= 0 {
		return time.Duration(defaultFailureRunningLeaseSeconds) * time.Second
	}
	return time.Duration(m.cfg.FailureRetry.RunningLeaseSeconds) * time.Second
}

// processorTimeout 返回指定业务 Processor 的单批执行超时。
func (m *Manager) processorTimeout(bizType string) time.Duration {
	seconds := 0
	if m != nil {
		if task, ok := m.cfg.Tasks[strings.TrimSpace(bizType)]; ok {
			seconds = task.ProcessorTimeoutSeconds
		}
		if seconds <= 0 {
			seconds = m.cfg.DefaultTask.ProcessorTimeoutSeconds
		}
	}
	if seconds <= 0 {
		return defaultProcessorTimeout
	}
	timeout := time.Duration(seconds) * time.Second
	if timeout > maxCollectorProcessorTimeout {
		return maxCollectorProcessorTimeout
	}
	return timeout
}

// collectorIdempotencyTTL 返回成功或失败终态去重缓存 TTL。
func collectorIdempotencyTTL(cfg config.CollectorIdempotencyConfig) time.Duration {
	if cfg.TTLSeconds <= 0 {
		return defaultIdempotencyTTL
	}
	return time.Duration(cfg.TTLSeconds) * time.Second
}

// collectorIdempotencyProcessingTTL 返回处理中占用租约 TTL。
func collectorIdempotencyProcessingTTL(cfg config.CollectorIdempotencyConfig) time.Duration {
	if cfg.ProcessingTTLSeconds <= 0 {
		return defaultIdempotencyProcessingTTL
	}
	return time.Duration(cfg.ProcessingTTLSeconds) * time.Second
}

// collectorIdempotencyPipelineBatchSize 返回 Redis Pipeline 分块命令数。
func collectorIdempotencyPipelineBatchSize(cfg config.CollectorIdempotencyConfig) int {
	return boundedPositiveInt(cfg.PipelineBatchSize, defaultIdempotencyPipelineBatchSize, maxIdempotencyPipelineBatchSize)
}

// collectorAnyIdempotencyEnabled 判断配置中是否存在需要 Redis 去重的任务。
func collectorAnyIdempotencyEnabled(cfg config.CollectorConfig) bool {
	if cfg.Idempotency.Enabled {
		return true
	}
	if cfg.DefaultTask.IdempotencyEnabled != nil && *cfg.DefaultTask.IdempotencyEnabled {
		return true
	}
	for _, task := range cfg.Tasks {
		if task.IdempotencyEnabled != nil && *task.IdempotencyEnabled {
			return true
		}
	}
	return false
}

// normalizeCollectorTaskRoutes 清理任务 Kafka 路由字段，避免空白值参与匹配。
func normalizeCollectorTaskRoutes(cfg *config.CollectorConfig) {
	if cfg == nil {
		return
	}
	cfg.DefaultTask.Topic = strings.TrimSpace(cfg.DefaultTask.Topic)
	cfg.DefaultTask.GroupID = strings.TrimSpace(cfg.DefaultTask.GroupID)
	if len(cfg.Tasks) == 0 {
		return
	}
	tasks := make(map[string]config.CollectorTaskConfig, len(cfg.Tasks))
	for bizType, task := range cfg.Tasks {
		bizType = strings.TrimSpace(bizType)
		task.Topic = strings.TrimSpace(task.Topic)
		task.GroupID = strings.TrimSpace(task.GroupID)
		tasks[bizType] = task
	}
	cfg.Tasks = tasks
}

// collectorConfiguredTopics 返回 Collector 当前配置需要写入的 Topic 列表。
func collectorConfiguredTopics(cfg config.CollectorConfig) []string {
	topics := make(map[string]struct{})
	if cfg.DefaultTask.Topic != "" {
		topics[cfg.DefaultTask.Topic] = struct{}{}
	}
	for _, task := range cfg.Tasks {
		topic := firstNonEmpty(task.Topic, cfg.DefaultTask.Topic)
		if topic == "" {
			continue
		}
		topics[topic] = struct{}{}
	}
	return sortedKeys(topics)
}

// collectorConfiguredReadRoutes 返回按 Topic 去重后的 Kafka 消费路由。
func collectorConfiguredReadRoutes(cfg config.CollectorConfig) []collectorKafkaRoute {
	byTopic := make(map[string]string)
	if cfg.DefaultTask.Topic != "" && cfg.DefaultTask.GroupID != "" {
		byTopic[cfg.DefaultTask.Topic] = cfg.DefaultTask.GroupID
	}
	for _, task := range cfg.Tasks {
		topic := firstNonEmpty(task.Topic, cfg.DefaultTask.Topic)
		groupID := firstNonEmpty(task.GroupID, cfg.DefaultTask.GroupID)
		if topic == "" || groupID == "" {
			continue
		}
		byTopic[topic] = groupID
	}
	topics := sortedKeysFromStringMap(byTopic)
	routes := make([]collectorKafkaRoute, 0, len(topics))
	for _, topic := range topics {
		routes = append(routes, collectorKafkaRoute{topic: topic, groupID: byTopic[topic]})
	}
	return routes
}

// sortedKeys 返回字符串集合的稳定排序结果。
func sortedKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// sortedKeysFromStringMap 返回字符串映射的稳定 key 顺序。
func sortedKeysFromStringMap(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// sortedWriterTopics 返回 Kafka writer 的稳定 Topic 顺序。
func sortedWriterTopics(writers map[string]kafkaMessageWriter) []string {
	topics := make([]string, 0, len(writers))
	for topic := range writers {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}

// firstNonEmpty 返回第一个非空白字符串。
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

// boundedPositiveInt 返回带上限的正整数配置，避免误配置过大批次拖垮 DB 或内存。
func boundedPositiveInt(value int, fallback int, maxValue int) int {
	if fallback <= 0 {
		fallback = 1
	}
	if maxValue <= 0 {
		maxValue = fallback
	}
	if value <= 0 {
		value = fallback
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

// failureSeedsStateLabel 归一化一批失败事件的初始状态标签，便于 metrics 聚合。
func failureSeedsStateLabel(seeds []failureEventSeed) string {
	if len(seeds) == 0 {
		return "unknown"
	}
	label := failedEventStateMetricLabel(seeds[0].state)
	for i := 1; i < len(seeds); i++ {
		if current := failedEventStateMetricLabel(seeds[i].state); current != label {
			return "mixed"
		}
	}
	return label
}

// failedEventStateMetricLabel 返回失败账本状态的指标标签。
func failedEventStateMetricLabel(state model.CollectorFailedEventState) string {
	switch state {
	case model.CollectorFailedEventStatePending:
		return "pending"
	case model.CollectorFailedEventStateRunning:
		return "running"
	case model.CollectorFailedEventStateDone:
		return "done"
	case model.CollectorFailedEventStateRetry:
		return "retry"
	case model.CollectorFailedEventStateDead:
		return "dead"
	default:
		return "unknown"
	}
}

// truncateErr 清理并按 UTF-8 字节边界截断错误摘要，保证可写入 utf8mb4 字段。
func truncateErr(msg string, limit int) string {
	return strings.TrimSpace(truncateTextBytes(strings.TrimSpace(msg), limit))
}

// truncateTextBytes 把任意字节串转成有效 UTF-8，并返回不超过上限的完整字符前缀。
func truncateTextBytes(text string, limit int) string {
	if limit <= 0 {
		return ""
	}
	text = strings.ToValidUTF8(text, "�")
	if len(text) <= limit {
		return text
	}
	end := limit
	for end > 0 && !utf8.ValidString(text[:end]) {
		end--
	}
	return text[:end]
}

// collectorAlertReason 生成 Collector 告警摘要，保留样例 ID 但不让它参与告警指纹。
func collectorAlertReason(sampleLabel, sampleValue, reason string) string {
	parts := make([]string, 0, 2)
	sampleLabel = strings.TrimSpace(sampleLabel)
	sampleValue = strings.TrimSpace(sampleValue)
	if sampleLabel != "" && sampleValue != "" {
		parts = append(parts, sampleLabel+"="+sampleValue)
	}
	if reason = strings.TrimSpace(reason); reason != "" {
		parts = append(parts, reason)
	}
	return strings.Join(parts, "；")
}

// logSlowBatch 记录慢批次，便于压测和生产环境定位瓶颈。
func (m *Manager) logSlowBatch(ctx context.Context, stage string, size int, beginAt time.Time) {
	if size <= 0 || beginAt.IsZero() {
		return
	}
	cost := time.Since(beginAt)
	if cost < slowCollectorBatchThreshold {
		return
	}
	loggerx.Sloww(ctx, "采集器 批次耗时较高",
		logx.Field("stage", stage),
		logx.Field("batch_size", size),
		logx.Field("latency_ms", cost.Milliseconds()),
	)
}

// reportRuntimeAlert 上报 Collector 后台运行异常；未配置 hook 时保持原有日志行为。
func (m *Manager) reportRuntimeAlert(ctx context.Context, alert RuntimeAlert) {
	if m == nil {
		return
	}
	m.mu.RLock()
	hook := m.alertHook
	m.mu.RUnlock()
	if hook == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	hook(ctx, normalizeRuntimeAlert(alert))
}
