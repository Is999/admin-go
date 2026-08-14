package admin

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
	"time"
	"unicode"

	codes "admin/common/codes"
	i18n "admin/common/i18n"
	keys "admin/common/rediskeys"
	"admin/internal/types"

	"github.com/Is999/go-utils/errors"
	"github.com/google/uuid"
)

const (
	// loginCaptchaTTLSeconds 表示登录图形验证码有效期（秒）。
	loginCaptchaTTLSeconds = 300
	// loginCaptchaLength 表示图形验证码长度。
	loginCaptchaLength = 4
)

// loginCaptchaAlphabet 定义大小写不敏感的验证码基础字符；字母生成后随机显示为大写或小写，避免重复字符降低有效答案空间。
var loginCaptchaAlphabet = []rune("ABCDEFGHJKLMNPQRSTUVWXYZ23456789")

// BuildLoginCaptcha 生成登录图形验证码并写入 Redis。
func (l *AdminLogic) BuildLoginCaptcha() *types.BizResult {
	if l.Redis() == nil {
		return types.NewBizResult(codes.ServerError).
			SetI18nMessage(i18n.MsgKeyInternalError).
			WithError(errors.New("AdminLogic.BuildLoginCaptcha Redis 未初始化"))
	}
	key := strings.ReplaceAll(uuid.NewString(), "-", "")
	code, err := generateLoginCaptchaCode(loginCaptchaLength)
	if err != nil {
		return types.NewBizResult(codes.ServerError).
			SetI18nMessage(i18n.MsgKeyInternalError).
			WithError(errors.Wrap(err, "AdminLogic.BuildLoginCaptcha 生成验证码失败"))
	}
	image, err := buildLoginCaptchaImageDataURL(code)
	if err != nil {
		return types.NewBizResult(codes.ServerError).
			SetI18nMessage(i18n.MsgKeyInternalError).
			WithError(errors.Wrap(err, "AdminLogic.BuildLoginCaptcha 渲染验证码失败"))
	}
	cacheKey := l.AppRedisKey(fmt.Sprintf(keys.LoginCaptcha, key))
	if err = l.Redis().Set(l.Ctx, cacheKey, strings.ToLower(code), loginCaptchaTTLSeconds*time.Second).Err(); err != nil {
		return types.NewBizResult(codes.ServerError).
			SetI18nMessage(i18n.MsgKeyInternalErrorFormat).
			WithError(errors.Wrap(err, "AdminLogic.BuildLoginCaptcha 写入验证码缓存失败"))
	}
	return types.NewBizResult(codes.Success).
		SetI18nMessage(i18n.MsgKeySuccess).
		WithData(&types.LoginCaptchaResp{
			Key:           key,
			Image:         image,
			ExpireSeconds: loginCaptchaTTLSeconds,
		})
}

// VerifyLoginCaptcha 校验登录图形验证码；校验通过或失败后都会消费当前验证码，避免重复重放。
func (l *AdminLogic) VerifyLoginCaptcha(key string, captcha string) *types.BizResult {
	if l.Redis() == nil {
		return types.NewBizResult(codes.ServerError).
			SetI18nMessage(i18n.MsgKeyInternalError).
			WithError(errors.New("AdminLogic.VerifyLoginCaptcha Redis 未初始化"))
	}
	key = strings.TrimSpace(key)
	captcha = strings.ToLower(strings.TrimSpace(captcha))
	if key == "" || captcha == "" {
		return invalidLoginCaptchaResult(errors.New("登录验证码不能为空"))
	}
	cacheKey := l.AppRedisKey(fmt.Sprintf(keys.LoginCaptcha, key))
	saved, err := l.Redis().GetDel(l.Ctx, cacheKey).Result()
	if err != nil {
		return invalidLoginCaptchaResult(errors.Wrap(err, "AdminLogic.VerifyLoginCaptcha 读取验证码缓存失败"))
	}
	if strings.TrimSpace(saved) == "" || !strings.EqualFold(saved, captcha) {
		return invalidLoginCaptchaResult(errors.Errorf("AdminLogic.VerifyLoginCaptcha 验证码错误 key=%s", key))
	}
	return types.NewBizResult(codes.Success).
		SetI18nMessage(i18n.MsgKeySuccess)
}

// invalidLoginCaptchaResult 统一构造登录验证码无效响应。
func invalidLoginCaptchaResult(err error) *types.BizResult {
	return types.NewBizResult(codes.InvalidCaptcha).
		SetI18nMessage(i18n.MsgKeyInvalidCaptcha).
		WithError(err)
}

// generateLoginCaptchaCode 生成指定长度的验证码文本。
func generateLoginCaptchaCode(length int) (string, error) {
	if length <= 0 {
		length = loginCaptchaLength
	}
	builder := strings.Builder{}
	for range length {
		index, err := randomInt(len(loginCaptchaAlphabet))
		if err != nil {
			return "", errors.Tag(err)
		}
		character := loginCaptchaAlphabet[index]
		if unicode.IsLetter(character) {
			letterCase, caseErr := randomInt(2)
			if caseErr != nil {
				return "", errors.Tag(caseErr)
			}
			if letterCase == 1 {
				character = unicode.ToLower(character)
			}
		}
		builder.WriteRune(character)
	}
	return builder.String(), nil
}

// randomInt 生成 [0, max) 的安全随机整数。
func randomInt(max int) (int, error) {
	if max <= 0 {
		return 0, errors.New("max 必须大于 0")
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		return 0, errors.Tag(err)
	}
	return int(n.Int64()), nil
}
