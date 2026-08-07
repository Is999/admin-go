package prometheusx

import (
	"github.com/Is999/go-utils/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Register 注册项目指标并返回实际生效的 Collector；同类型重复注册时复用已存在实例，其它冲突返回启动错误。
func Register[T prometheus.Collector](collector T) (T, error) {
	if err := prometheus.Register(collector); err != nil {
		var duplicate prometheus.AlreadyRegisteredError
		if !errors.As(err, &duplicate) {
			var zero T
			return zero, errors.Wrapf(err, "注册 Prometheus 指标失败 collector=%T", collector)
		}
		existing, ok := duplicate.ExistingCollector.(T)
		if !ok {
			var zero T
			return zero, errors.Errorf("Prometheus 同名指标类型不一致 new=%T existing=%T", collector, duplicate.ExistingCollector)
		}
		return existing, nil
	}
	return collector, nil
}
