package admin

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"image"
	"image/color"
	"image/gif"
	"math"
	"net/http"
	"strings"
	"testing"

	codes "admin/common/codes"
	keys "admin/common/rediskeys"
	"admin/internal/config"
	corelogic "admin/internal/logic"
	"admin/internal/svc"
	"admin/internal/types"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestAdminCaptchaLogic 创建仅包含 Redis 的登录验证码测试逻辑对象。
func newTestAdminCaptchaLogic(t *testing.T) (*AdminLogic, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	svcCtx := svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{Rds: client})
	return &AdminLogic{BaseLogic: corelogic.NewBaseLogicWithContext(context.Background(), svcCtx)}, server
}

// TestBuildLoginCaptchaAndVerify 验证登录验证码可成功生成、校验并在成功后立即失效。
func TestBuildLoginCaptchaAndVerify(t *testing.T) {
	logicObj, _ := newTestAdminCaptchaLogic(t)
	resp := logicObj.BuildLoginCaptcha()
	if resp == nil || resp.IsFailure() {
		t.Fatalf("BuildLoginCaptcha() = %#v, want success", resp)
	}
	data, ok := resp.Data.(*types.LoginCaptchaResp)
	if !ok || data == nil {
		t.Fatalf("BuildLoginCaptcha() data = %#v, want *types.LoginCaptchaResp", resp.Data)
	}
	if strings.TrimSpace(data.Key) == "" || strings.TrimSpace(data.Image) == "" {
		t.Fatalf("BuildLoginCaptcha() returned empty key or image: %#v", data)
	}
	parts := strings.SplitN(data.Image, ",", 2)
	if len(parts) != 2 || parts[0] != "data:image/gif;base64" {
		t.Fatalf("验证码图片必须是 GIF data URL，实际前缀为 %q", parts[0])
	}
	imageBytes, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("解码验证码 GIF 失败: %v", err)
	}
	if contentType := http.DetectContentType(imageBytes); contentType != "image/gif" {
		t.Fatalf("验证码图片类型=%q，期望 image/gif", contentType)
	}
	animation, err := gif.DecodeAll(bytes.NewReader(imageBytes))
	if err != nil {
		t.Fatalf("读取验证码 GIF 帧失败: %v", err)
	}
	if animation.Config.Width != loginCaptchaImageWidth || animation.Config.Height != loginCaptchaImageHeight {
		t.Fatalf("验证码图片尺寸=%dx%d，期望 %dx%d", animation.Config.Width, animation.Config.Height, loginCaptchaImageWidth, loginCaptchaImageHeight)
	}
	if len(animation.Image) != loginCaptchaAnimationFrameCount || len(animation.Delay) != loginCaptchaAnimationFrameCount {
		t.Fatalf("验证码 GIF 帧数 image=%d delay=%d，期望 %d", len(animation.Image), len(animation.Delay), loginCaptchaAnimationFrameCount)
	}
	for frameIndex, delay := range animation.Delay {
		if delay != loginCaptchaFrameDelayCentiseconds {
			t.Fatalf("验证码 GIF 第 %d 帧延迟=%d，期望 %d 厘秒", frameIndex, delay, loginCaptchaFrameDelayCentiseconds)
		}
	}
	if bytes.Equal(animation.Image[0].Pix, animation.Image[1].Pix) {
		t.Fatal("验证码 GIF 相邻帧不应相同，彩虹波浪带必须发生移动")
	}
	if bytes.Contains(imageBytes, []byte("<text")) || bytes.Contains(imageBytes, []byte("<svg")) {
		t.Fatal("验证码图片不应包含可直接解析的 SVG 明文节点")
	}
	cacheKey := logicObj.AppRedisKey(fmt.Sprintf(keys.LoginCaptcha, data.Key))
	code, err := logicObj.Redis().Get(context.Background(), cacheKey).Result()
	if err != nil {
		t.Fatalf("Get(%s) error = %v", cacheKey, err)
	}
	verifyResp := logicObj.VerifyLoginCaptcha(data.Key, code)
	if verifyResp == nil || verifyResp.IsFailure() {
		t.Fatalf("VerifyLoginCaptcha(success) = %#v, want success", verifyResp)
	}
	verifyAgainResp := logicObj.VerifyLoginCaptcha(data.Key, code)
	if verifyAgainResp == nil || verifyAgainResp.Code != codes.InvalidCaptcha {
		t.Fatalf("VerifyLoginCaptcha(reuse) = %#v, want invalid captcha", verifyAgainResp)
	}
}

// TestVerifyLoginCaptchaRejectsWrongCode 验证错误验证码会被拒绝，并消费掉当前验证码。
func TestVerifyLoginCaptchaRejectsWrongCode(t *testing.T) {
	logicObj, _ := newTestAdminCaptchaLogic(t)
	resp := logicObj.BuildLoginCaptcha()
	data := resp.Data.(*types.LoginCaptchaResp)
	verifyResp := logicObj.VerifyLoginCaptcha(data.Key, "WRONG")
	if verifyResp == nil || verifyResp.Code != codes.InvalidCaptcha {
		t.Fatalf("VerifyLoginCaptcha(wrong) = %#v, want invalid captcha", verifyResp)
	}
	cacheKey := logicObj.AppRedisKey(fmt.Sprintf(keys.LoginCaptcha, data.Key))
	if logicObj.Redis().Exists(context.Background(), cacheKey).Val() != 0 {
		t.Fatalf("VerifyLoginCaptcha(wrong) should consume captcha key %s", cacheKey)
	}
}

// TestDrawLoginCaptchaBaseFrameKeepsTextInsideImage 验证宽字符在动态图片的基础帧内不会贴边裁切。
func TestDrawLoginCaptchaBaseFrameKeepsTextInsideImage(t *testing.T) {
	for range 20 {
		captchaImage, err := drawLoginCaptchaBaseFrame("WMWM")
		if err != nil {
			t.Fatalf("drawLoginCaptchaBaseFrame() error = %v", err)
		}
		bounds := captchaImage.Bounds()
		for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
			for x := bounds.Min.X; x < bounds.Max.X; x++ {
				red, green, blue, _ := captchaImage.At(x, y).RGBA()
				if !isLoginCaptchaTextPixel(red, green, blue) {
					continue
				}
				if x <= bounds.Min.X+1 || x >= bounds.Max.X-2 || y <= bounds.Min.Y+1 || y >= bounds.Max.Y-2 {
					t.Fatalf("验证码深色文字像素贴边: x=%d y=%d bounds=%v", x, y, bounds)
				}
			}
		}
	}
}

// TestDrawLoginCaptchaRainbowBandUsesOpaqueMovingLines 验证实体线覆盖当前像素且随波浪带移出后不再残留。
func TestDrawLoginCaptchaRainbowBandUsesOpaqueMovingLines(t *testing.T) {
	background := color.NRGBA{R: 255, G: 255, B: 255, A: 255}
	canvas := image.NewNRGBA(image.Rect(0, 0, loginCaptchaImageWidth, loginCaptchaImageHeight))
	for y := 0; y < loginCaptchaImageHeight; y++ {
		for x := 0; x < loginCaptchaImageWidth; x++ {
			canvas.SetNRGBA(x, y, background)
		}
	}

	const (
		left       = 10
		frameIndex = 0
		bandIndex  = 0
		y          = loginCaptchaImageHeight / 2
	)
	drawLoginCaptchaRainbowBand(canvas, left, frameIndex, bandIndex, loginCaptchaRainbowPrimaryAlpha)
	slant := math.Tan(float64(loginCaptchaRainbowSlantDegrees) * math.Pi / 180)
	wave := int(math.Round(float64(loginCaptchaRainbowWaveAmplitude) * math.Sin(float64(y)*4*math.Pi/float64(loginCaptchaImageHeight))))
	slantOffset := int(math.Round(float64(y-loginCaptchaImageHeight/2) * slant))
	startX := left + wave + slantOffset
	for lineIndex := range loginCaptchaRainbowSolidLineCount {
		lineX := startX + (lineIndex+1)*loginCaptchaRainbowBandWidth/(loginCaptchaRainbowSolidLineCount+1)
		expected := loginCaptchaRainbowColors[(frameIndex+bandIndex*3+lineIndex*3+y/12)%len(loginCaptchaRainbowColors)]
		if actual := canvas.NRGBAAt(lineX, y); actual != color.NRGBAModel.Convert(expected).(color.NRGBA) {
			t.Fatalf("第 %d 条实体线像素=%#v，期望完全不透明颜色 %#v", lineIndex, actual, expected)
		}
	}

	revealed := image.NewNRGBA(canvas.Bounds())
	for y := 0; y < loginCaptchaImageHeight; y++ {
		for x := 0; x < loginCaptchaImageWidth; x++ {
			revealed.SetNRGBA(x, y, background)
		}
	}
	drawLoginCaptchaRainbowBand(revealed, -loginCaptchaImageWidth, frameIndex, bandIndex, loginCaptchaRainbowPrimaryAlpha)
	if actual := revealed.NRGBAAt(startX, y); actual != background {
		t.Fatalf("波浪带移开后像素=%#v，期望恢复原始背景 %#v", actual, background)
	}
}

// TestDrawLoginCaptchaHorizontalRainbowLineMoves 验证横向彩虹线逐帧改变位置且使用完全不透明像素。
func TestDrawLoginCaptchaHorizontalRainbowLineMoves(t *testing.T) {
	newCanvas := func() *image.NRGBA {
		canvas := image.NewNRGBA(image.Rect(0, 0, loginCaptchaImageWidth, loginCaptchaImageHeight))
		for y := 0; y < loginCaptchaImageHeight; y++ {
			for x := 0; x < loginCaptchaImageWidth; x++ {
				canvas.SetNRGBA(x, y, color.NRGBA{R: 255, G: 255, B: 255, A: 255})
			}
		}
		return canvas
	}
	first := newCanvas()
	second := newCanvas()
	drawLoginCaptchaHorizontalRainbowLine(first, 0)
	drawLoginCaptchaHorizontalRainbowLine(second, loginCaptchaAnimationFrameCount/4)
	if bytes.Equal(first.Pix, second.Pix) {
		t.Fatal("横向彩虹线在四分之一周期后必须改变位置")
	}
	for _, canvas := range []*image.NRGBA{first, second} {
		foundOpaqueColor := false
		for pixelIndex := 0; pixelIndex < len(canvas.Pix); pixelIndex += 4 {
			if canvas.Pix[pixelIndex] == 255 && canvas.Pix[pixelIndex+1] == 255 && canvas.Pix[pixelIndex+2] == 255 {
				continue
			}
			if canvas.Pix[pixelIndex+3] != 255 {
				t.Fatalf("横向彩虹线像素 alpha=%d，期望 255", canvas.Pix[pixelIndex+3])
			}
			foundOpaqueColor = true
		}
		if !foundOpaqueColor {
			t.Fatal("横向彩虹线未绘制任何彩色像素")
		}
	}
}

// TestLoginCaptchaCharactersHideLeftToRightAndJump 验证每 3 帧只隐藏一个字符，隐藏位置从左到右推进且字符轨迹发生跳动。
func TestLoginCaptchaCharactersHideLeftToRightAndJump(t *testing.T) {
	const code = "1234"
	random := captchaImageRandom{}
	render := func(frameIndex int, hiddenCharacterIndex int) *image.NRGBA {
		canvas := image.NewNRGBA(image.Rect(0, 0, loginCaptchaImageWidth, loginCaptchaImageHeight))
		for y := 0; y < loginCaptchaImageHeight; y++ {
			for x := 0; x < loginCaptchaImageWidth; x++ {
				canvas.SetNRGBA(x, y, color.NRGBA{R: 255, G: 255, B: 255, A: 255})
			}
		}
		frameRandom := random
		if err := drawLoginCaptchaText(canvas, code, &frameRandom, frameIndex, hiddenCharacterIndex); err != nil {
			t.Fatalf("drawLoginCaptchaText(frame=%d hidden=%d) error = %v", frameIndex, hiddenCharacterIndex, err)
		}
		return canvas
	}
	cellWidth := (loginCaptchaImageWidth - loginCaptchaPaddingX*2) / len([]rune(code))
	for frameIndex := range loginCaptchaAnimationFrameCount {
		hiddenIndex := loginCaptchaHiddenCharacterIndex(frameIndex, len([]rune(code)))
		expectedHiddenIndex := frameIndex / (loginCaptchaAnimationFrameCount / len([]rune(code)))
		if hiddenIndex != expectedHiddenIndex {
			t.Fatalf("第 %d 帧隐藏字符=%d，期望从左到右推进到 %d", frameIndex, hiddenIndex, expectedHiddenIndex)
		}
		allVisible := render(frameIndex, -1)
		oneHidden := render(frameIndex, hiddenIndex)
		for characterIndex := range len([]rune(code)) {
			startX := loginCaptchaPaddingX + characterIndex*cellWidth
			endX := startX + cellWidth
			allVisiblePixels := countLoginCaptchaDarkPixels(allVisible, startX, endX)
			hiddenPixels := countLoginCaptchaDarkPixels(oneHidden, startX, endX)
			if characterIndex == hiddenIndex {
				if allVisiblePixels == 0 || hiddenPixels != 0 {
					t.Fatalf("第 %d 帧字符 %d 隐藏像素=%d，完整像素=%d", frameIndex, characterIndex, hiddenPixels, allVisiblePixels)
				}
				continue
			}
			if hiddenPixels != allVisiblePixels {
				t.Fatalf("第 %d 帧误改非目标字符 %d：隐藏帧像素=%d，完整帧像素=%d", frameIndex, characterIndex, hiddenPixels, allVisiblePixels)
			}
		}
	}
	if bytes.Equal(render(0, -1).Pix, render(loginCaptchaAnimationFrameCount/4, -1).Pix) {
		t.Fatal("字符在四分之一周期后必须改变纵向位置")
	}
}

// countLoginCaptchaDarkPixels 统计指定字符槽位内的深色文字像素。
func countLoginCaptchaDarkPixels(canvas *image.NRGBA, startX int, endX int) int {
	count := 0
	for y := 0; y < loginCaptchaImageHeight; y++ {
		for x := startX; x < endX; x++ {
			red, green, blue, _ := canvas.At(x, y).RGBA()
			if isLoginCaptchaTextPixel(red, green, blue) {
				count++
			}
		}
	}
	return count
}

// isLoginCaptchaTextPixel 判断像素是否属于深色文字区域。
func isLoginCaptchaTextPixel(red uint32, green uint32, blue uint32) bool {
	return int(red>>8)+int(green>>8)+int(blue>>8) < 420
}
