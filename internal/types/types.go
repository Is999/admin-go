//lint:file-ignore SA5008 ignore go-zero optional tag

package types

import (
	"context"

	codes "admin/common/codes"
	i18n "admin/common/i18n"
	"admin/internal/requestctx"

	"github.com/Is999/go-utils/errors"
)

// Nil 表示一个显式的空业务错误占位值，用于区分“无错误对象”和“失败但无需暴露原始错误”。
const Nil = BizError("Biz: nil")

// BizError 通用业务错误类型
type BizError string

// Error 返回业务错误文本，实现 error 接口。
func (e BizError) Error() string { return string(e) }

// BizResult 通用响应结构体，支持泛型
type BizResult struct {
	Code        int    `json:"-"` // 响应代码: 1 成功, 2 失败 ... 在codes包定义
	MessageKey  string `json:"-"` // 国际化消息 key（推荐）
	MessageArgs []any  `json:"-"` // 国际化消息参数
	Error       error  `json:"-"` // 错误信息
	Req         any    `json:"-"` // 请求参数
	Data        any    `json:"-"` // 响应数据
}

// CacheSyncResp 表示数据库操作已提交但缓存仍待同步。
type CacheSyncResp struct {
	SyncPending bool `json:"syncPending"` // 是否仍有缓存同步未完成
}

// NewBizResult 创建业务响应基础对象，后续可通过链式方法补充 message/data/error。
func NewBizResult(code int) *BizResult {
	return &BizResult{Code: code}
}

// IsSuccess 判断当前业务结果是否可视为成功。
// 业务码必须属于成功集合，且不能附带实际错误对象。
func (r *BizResult) IsSuccess() bool {
	if r == nil {
		return false
	}
	return codes.IsSuccess(r.Code) && r.Error == nil
}

// IsFailure 判断当前业务结果是否为失败。
func (r *BizResult) IsFailure() bool {
	return !r.IsSuccess()
}

// SetI18nMessage 设置国际化消息 key 与参数。
func (r *BizResult) SetI18nMessage(key string, args ...any) *BizResult {
	if r == nil {
		return r
	}
	r.MessageKey = key
	r.MessageArgs = args
	return r
}

// WithError 设置业务错误对象，供统一日志和失败响应分支使用。
func (r *BizResult) WithError(err error) *BizResult {
	if r == nil {
		return r
	}
	if err == nil || errors.Is(err, Nil) {
		r.Error = err
		return r
	}
	r.Error = errors.Tag(err)
	return r
}

// WithReq 设置原始请求对象，供审计日志投递时回放关键请求参数。
func (r *BizResult) WithReq(req any) *BizResult {
	if r == nil {
		return r
	}
	r.Req = req
	return r
}

// WithData 设置响应数据负载。
func (r *BizResult) WithData(data any) *BizResult {
	if r == nil {
		return r
	}
	r.Data = data
	return r
}

// ParamErrorResult 统一构造参数错误响应，并挂上国际化模板消息。
func ParamErrorResult(err error) *BizResult {
	if err == nil {
		return NewBizResult(codes.ParamError).WithError(Nil).SetI18nMessage(i18n.MsgKeyParamError)
	}
	message := err.Error()
	return NewBizResult(codes.ParamError).WithError(Nil).SetI18nMessage(i18n.MsgKeyParamErrorFormat, message)
}

// ResolveMessage 按“MessageKey > Code 默认文案”的优先级解析最终响应文案。
func (r *BizResult) ResolveMessage(ctx context.Context) string {
	if r == nil {
		return ""
	}
	locale := i18n.LocaleZHCN
	if meta := requestctx.FromContext(ctx); meta != nil && meta.Locale != "" {
		locale = meta.Locale
	}

	if r.MessageKey != "" {
		return i18n.MessageByKey(r.MessageKey, locale, r.MessageArgs...)
	}
	return i18n.MessageByCode(r.Code, locale)
}

const (
	// defaultPageNumber 表示管理后台分页的默认页码，未传或传入非法页码时回到第一页。
	defaultPageNumber = 1
	// defaultPageSize 表示管理后台分页的默认单页数量，兼顾首屏响应速度和常规运营查看习惯。
	defaultPageSize = 10
	// maxPageSize 表示管理后台通用分页的最大单页数量，避免异常请求一次拉取过多记录拖慢数据库。
	maxPageSize = 100
)

// normalizePage 归一化页码和每页数量，调用方可按业务场景指定默认每页数量。
func normalizePage(page, pageSize, defaultSize int) (int, int) {
	return normalizePageWithMax(page, pageSize, defaultSize, maxPageSize)
}

// normalizePageWithMax 归一化页码和每页数量，maxSize 只能由有明确查询负载边界的业务列表指定。
func normalizePageWithMax(page, pageSize, defaultSize, maxSize int) (int, int) {
	if page < 1 {
		page = defaultPageNumber
	}
	if pageSize < 1 {
		pageSize = defaultSize
	}
	if maxSize < 1 {
		maxSize = maxPageSize
	}
	if pageSize > maxSize {
		pageSize = maxSize
	}
	return page, pageSize
}

// GetPageReq 表示通用 GET 分页请求参数。
type GetPageReq struct {
	Page     int `form:"page,default=1"`      // 页码
	PageSize int `form:"pageSize,default=10"` // 每页数量
}

// Validate 校验并归一化分页参数。
func (r *GetPageReq) Validate() error {
	r.Page, r.PageSize = normalizePage(r.Page, r.PageSize, defaultPageSize)
	return nil
}

// PostPageReq 表示通用 POST 分页请求参数。
type PostPageReq struct {
	Page     int `json:"page"`     // 页码
	PageSize int `json:"pageSize"` // 每页数量
}

// Validate 校验并归一化分页参数。
func (r *PostPageReq) Validate() error {
	r.Page, r.PageSize = normalizePage(r.Page, r.PageSize, defaultPageSize)
	return nil
}

// GetOrderReq 表示通用 GET 排序请求参数。
type GetOrderReq struct {
	OrderBy string `form:"orderBy,optional"` // 排序字段，非必填
	Order   string `form:"order,optional"`   // 排序方式，非必填，asc|desc
}

// Validate 校验并归一化排序参数。
func (r *GetOrderReq) Validate() error {
	if r.Order != "" && r.Order != "asc" && r.Order != "desc" {
		r.Order = "desc"
	}
	if r.OrderBy != "" && r.Order == "" {
		r.Order = "desc"
	}
	return nil
}

// PostOrderReq 表示通用 POST 排序请求参数。
type PostOrderReq struct {
	OrderBy string `json:"orderBy,optional"` // 排序字段，非必填
	Order   string `json:"order,optional"`   // 排序方式，非必填，asc|desc
}

// Validate 校验并归一化排序参数。
func (r *PostOrderReq) Validate() error {
	if r.Order != "" && r.Order != "asc" && r.Order != "desc" {
		r.Order = "desc"
	}
	if r.OrderBy != "" && r.Order == "" {
		r.Order = "desc"
	}
	return nil
}

// ListResp 表示通用列表响应结构体。
type ListResp[T any] struct {
	List  []T   `json:"list,omitempty"`  // 列表数据
	Total int64 `json:"total,omitempty"` // 总数
	Meta  any   `json:"meta,omitempty"`  // 附加元数据（如分页信息、统计项等）
}

// DropdownItem 表示下拉框选项。
type DropdownItem struct {
	ID    any    `json:"id,omitempty"`   // 选项标识
	Label string `json:"label"`          // 名称
	Value any    `json:"value"`          // 选项值
	Meta  any    `json:"meta,omitempty"` // 附加元数据
}
