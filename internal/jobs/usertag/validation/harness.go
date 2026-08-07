package validation

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/Is999/go-utils/errors"
)

// SourceSummary 表示单个数据源的校验摘要。
type SourceSummary struct {
	Source string         // 数据源名称，例如 mysql/clickhouse/api/admin
	UID    int64          // 用户 UID
	Fields map[string]any // 关键字段摘要，map key 必须使用业务字段英文名
}

// Diff 表示一次校验发现的差异。
type Diff struct {
	UID     int64  // 用户 UID
	Field   string // 差异字段
	Left    any    // 左侧值
	Right   any    // 右侧值
	Comment string // 中文定位说明
}

// Report 表示 UID 校验报告。
type Report struct {
	WorkflowID string          // 工作流 ID
	Start      time.Time       // 校验窗口开始时间
	End        time.Time       // 校验窗口结束时间
	Summaries  []SourceSummary // 多侧数据源摘要
	Diffs      []Diff          // 差异列表
}

// Source 定义只读校验数据源接口。
// API 与 Admin 只能通过 adapter 参与校验，不能成为用户标签任务的运行时强依赖。
type Source interface {
	Name() string                                                                          // 数据源名称
	Load(ctx context.Context, uids []int64, start, end time.Time) ([]SourceSummary, error) // 批量加载校验摘要
}

// Harness 是 usertag 多侧校验入口。
type Harness struct {
	sources []Source // 只读校验数据源列表
}

// NewHarness 创建校验入口。
func NewHarness(sources ...Source) *Harness {
	return &Harness{sources: append([]Source(nil), sources...)}
}

// ValidateUIDs 按 UID 列表执行多侧摘要校验。
func (h *Harness) ValidateUIDs(ctx context.Context, workflowID string, uids []int64, start, end time.Time) (Report, error) {
	report := Report{WorkflowID: workflowID, Start: start, End: end}
	for _, source := range h.sources {
		rows, err := source.Load(ctx, uids, start, end)
		if err != nil {
			return report, errors.Tag(err)
		}
		report.Summaries = append(report.Summaries, rows...)
	}
	report.Diffs = buildDiffs(report.Summaries)
	return report, nil
}

// buildDiffs 按 UID 和字段对比多个数据源摘要，输出可排查差异。
func buildDiffs(rows []SourceSummary) []Diff {
	grouped := make(map[int64][]SourceSummary)
	for _, row := range rows {
		if row.UID <= 0 {
			continue
		}
		grouped[row.UID] = append(grouped[row.UID], row)
	}
	diffs := make([]Diff, 0)
	for uid, group := range grouped {
		if len(group) < 2 {
			continue
		}
		fields := summaryFields(group)
		for _, field := range fields {
			baseValue, baseSource, ok := firstFieldValue(group, field)
			if !ok {
				continue
			}
			for _, item := range group {
				value, exists := item.Fields[field]
				if !exists || reflect.DeepEqual(baseValue, value) {
					continue
				}
				diffs = append(diffs, Diff{
					UID:     uid,
					Field:   field,
					Left:    fmt.Sprintf("%s=%v", baseSource, baseValue),
					Right:   fmt.Sprintf("%s=%v", item.Source, value),
					Comment: "多侧校验字段值不一致，请结合数据源条件和时间窗口定位",
				})
			}
		}
	}
	return diffs
}

// summaryFields 提取摘要中出现过的字段名。
func summaryFields(rows []SourceSummary) []string {
	seen := make(map[string]struct{})
	fields := make([]string, 0)
	for _, row := range rows {
		for field := range row.Fields {
			if _, ok := seen[field]; ok {
				continue
			}
			seen[field] = struct{}{}
			fields = append(fields, field)
		}
	}
	sort.Strings(fields)
	return fields
}

// firstFieldValue 返回某字段的首个基准值。
func firstFieldValue(rows []SourceSummary, field string) (any, string, bool) {
	for _, row := range rows {
		value, ok := row.Fields[field]
		if ok {
			return value, row.Source, true
		}
	}
	return nil, "", false
}
