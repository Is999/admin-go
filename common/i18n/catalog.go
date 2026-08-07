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

// messageCatalog 在 main 执行前加载编译进二进制的 JSON 语言包，启动后只读。
// 固定资产缺失或格式损坏表示发布物本身不可用；panic 被限定在包级匿名初始化器，请求、任务和热加载只能调用返回 error 的 helper。
var messageCatalog = func() map[string]localeMessageCatalog {
	catalog, err := loadMessageCatalog()
	if err != nil {
		panic(err)
	}
	return catalog
}()

// messageBundle 在包级匿名初始化器中注册固定语种文案，注册失败表示内嵌资产或受支持语种常量不一致。
var messageBundle = func() *goi18n.Bundle {
	bundle, err := buildMessageBundle(messageCatalog)
	if err != nil {
		panic(err)
	}
	return bundle
}()

// loadMessageCatalog 从不可变的 go:embed JSON 资产加载语言包；资产错误交由包级初始化器决定是否终止启动。
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
