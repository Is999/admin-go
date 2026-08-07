package i18n

import (
	"embed"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/Is999/go-utils/errors"
	goi18n "github.com/nicksnyder/go-i18n/v2/i18n"
	"golang.org/x/text/language"
)

// localeFS 嵌入后端多语言 JSON 资产。
//
//go:embed locales/active.*.json
var localeFS embed.FS

// localeMessageCatalog 表示从 JSON 语言资产加载的单个语种文案。
type localeMessageCatalog map[string]string

var (
	// messageCatalog 保存编译进二进制且已解析的 JSON 文案，启动后只读。
	// messageBundle 保存已注册固定语种的 go-i18n 查询对象，资产错误时仍保持非空以保护错误响应链。
	// messageCatalogErr 保存资产加载或注册错误，只允许由 ValidateCatalog 交给应用启动链处理。
	messageCatalog, messageBundle, messageCatalogErr = loadMessageState()
)

// loadMessageState 加载并注册固定语言资产；失败时保留非空兜底对象并把错误交给启动链返回。
func loadMessageState() (map[string]localeMessageCatalog, *goi18n.Bundle, error) {
	fallbackCatalog := make(map[string]localeMessageCatalog, len(supportedLocales))
	for _, locale := range supportedLocales {
		fallbackCatalog[locale] = localeMessageCatalog{}
	}
	fallbackBundle := goi18n.NewBundle(language.SimplifiedChinese)
	catalog, err := loadMessageCatalog()
	if err != nil {
		return fallbackCatalog, fallbackBundle, errors.Tag(err)
	}
	bundle, err := buildMessageBundle(catalog)
	if err != nil {
		return catalog, fallbackBundle, errors.Tag(err)
	}
	return catalog, bundle, nil
}

// ValidateCatalog 在应用创建资源前校验内嵌语言资产，错误由 main 启动链记录并有序退出。
func ValidateCatalog() error {
	return errors.Tag(messageCatalogErr)
}

// loadMessageCatalog 从不可变的 go:embed JSON 资产加载语言包；资产错误由 ValidateCatalog 返回启动链。
func loadMessageCatalog() (map[string]localeMessageCatalog, error) {
	catalog := make(map[string]localeMessageCatalog, len(supportedLocales))
	for _, locale := range supportedLocales {
		path := fmt.Sprintf("locales/active.%s.json", locale)
		data, err := localeFS.ReadFile(path)
		if err != nil {
			return nil, errors.Wrapf(err, "加载语言包失败 locale=%s path=%s", locale, path)
		}
		messages := localeMessageCatalog{}
		if err := json.Unmarshal(data, &messages); err != nil {
			return nil, errors.Wrapf(err, "解析语言包失败 locale=%s path=%s", locale, path)
		}
		catalog[locale] = messages
	}
	return catalog, nil
}

// buildMessageBundle 把已校验的内嵌语言包注册到 go-i18n Bundle；语种或文案错误由调用方处理。
func buildMessageBundle(catalog map[string]localeMessageCatalog) (*goi18n.Bundle, error) {
	bundle := goi18n.NewBundle(language.SimplifiedChinese)
	for _, locale := range supportedLocales {
		messages := catalog[locale]
		ids := make([]string, 0, len(messages))
		for id := range messages {
			ids = append(ids, id)
		}
		sort.Strings(ids)

		items := make([]*goi18n.Message, 0, len(ids))
		for _, id := range ids {
			items = append(items, &goi18n.Message{ID: id, Other: messages[id]})
		}
		tag, err := language.Parse(locale)
		if err != nil {
			return nil, errors.Wrapf(err, "解析语言标签失败 locale=%s", locale)
		}
		if err := bundle.AddMessages(tag, items...); err != nil {
			return nil, errors.Wrapf(err, "注册语言包失败 locale=%s", locale)
		}
	}
	return bundle, nil
}
