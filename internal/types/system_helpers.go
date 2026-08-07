package types

import (
	"bytes"
	"encoding/json"
	"strings"

	utils "github.com/Is999/go-utils"
	"github.com/Is999/go-utils/errors"
)

// UniquePositiveInts 对正整数列表去重并保持首次出现顺序。
func UniquePositiveInts(items []int) []int {
	result := make([]int, 0, len(items))
	for _, item := range items {
		if item <= 0 {
			continue
		}
		result = append(result, item)
	}
	return utils.Unique(result)
}

// RawJSONToString 把 JSON 原始值转成数据库 JSON 字段可接受的字符串。
func RawJSONToString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "null"
	}
	return string(raw)
}

// normalizeRequestJSONValue 把请求里的任意 JSON 值统一转成原始 JSON 字节。
func normalizeRequestJSONValue(value any, allowEmpty bool) (json.RawMessage, error) {
	if value == nil {
		if allowEmpty {
			return nil, nil
		}
		return nil, errors.Errorf("配置值不能为空")
	}

	var raw json.RawMessage
	switch v := value.(type) {
	case json.RawMessage:
		raw = append(json.RawMessage(nil), v...)
	case []byte:
		raw = append(json.RawMessage(nil), v...)
	case string:
		text := strings.TrimSpace(v)
		if json.Valid([]byte(text)) {
			raw = json.RawMessage(text)
			break
		}
		body, err := json.Marshal(v)
		if err != nil {
			return nil, errors.Tag(err)
		}
		raw = body
	default:
		body, err := json.Marshal(v)
		if err != nil {
			return nil, errors.Tag(err)
		}
		raw = body
	}

	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		if allowEmpty {
			return nil, nil
		}
		return nil, errors.Errorf("配置值不能为空")
	}
	return raw, nil
}
