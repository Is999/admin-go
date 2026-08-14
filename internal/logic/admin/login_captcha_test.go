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
	"time"
	"unicode"

	codes "admin/common/codes"
	keys "admin/common/rediskeys"
	"admin/internal/config"
	corelogic "admin/internal/logic"
	"admin/internal/svc"
	"admin/internal/types"

	"github.com/alicebob/miniredis/v2"
	"github.com/golang/freetype/truetype"
	"github.com/redis/go-redis/v9"
	"golang.org/x/image/font"
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
	if len(animation.Image) < loginCaptchaAnimationMinFrameCount || len(animation.Image) > loginCaptchaAnimationMaxFrameCount || len(animation.Delay) != len(animation.Image) {
		t.Fatalf("验证码 GIF 帧数 image=%d delay=%d，期望范围 %d-%d 且数量一致", len(animation.Image), len(animation.Delay), loginCaptchaAnimationMinFrameCount, loginCaptchaAnimationMaxFrameCount)
	}
	for frameIndex, delay := range animation.Delay {
		if delay < loginCaptchaMinFrameDelayCentiseconds || delay > loginCaptchaMaxFrameDelayCentiseconds {
			t.Fatalf("验证码 GIF 第 %d 帧延迟=%d，期望范围 %d-%d 厘秒", frameIndex, delay, loginCaptchaMinFrameDelayCentiseconds, loginCaptchaMaxFrameDelayCentiseconds)
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

// TestVerifyLoginCaptchaIgnoresLetterCase 验证主验证码显示大小写不会改变登录校验结果，且成功后仍单次消费。
func TestVerifyLoginCaptchaIgnoresLetterCase(t *testing.T) {
	logicObj, _ := newTestAdminCaptchaLogic(t)
	const (
		key       = "mixed-case"
		savedCode = "aB7w"
		inputCode = "Ab7W"
	)
	cacheKey := logicObj.AppRedisKey(fmt.Sprintf(keys.LoginCaptcha, key))
	if err := logicObj.Redis().Set(context.Background(), cacheKey, savedCode, time.Minute).Err(); err != nil {
		t.Fatalf("写入混合大小写验证码失败: %v", err)
	}
	resp := logicObj.VerifyLoginCaptcha(key, inputCode)
	if resp == nil || resp.IsFailure() {
		t.Fatalf("VerifyLoginCaptcha(%q) = %#v, want success", inputCode, resp)
	}
	if logicObj.Redis().Exists(context.Background(), cacheKey).Val() != 0 {
		t.Fatalf("VerifyLoginCaptcha(%q) 后未消费 key %s", inputCode, cacheKey)
	}
}

// TestDrawLoginCaptchaBaseFrameKeepsTextInsideImage 验证宽字符和下伸字符在全部运动相位中始终保留 3px 四边空白。
func TestDrawLoginCaptchaBaseFrameKeepsTextInsideImage(t *testing.T) {
	for _, code := range []string{"WMWM", "agyp"} {
		for frameIndex := range loginCaptchaAnimationMaxFrameCount {
			captchaImage := image.NewNRGBA(image.Rect(0, 0, loginCaptchaImageWidth, loginCaptchaImageHeight))
			for pixelIndex := 0; pixelIndex < len(captchaImage.Pix); pixelIndex += 4 {
				captchaImage.Pix[pixelIndex] = 255
				captchaImage.Pix[pixelIndex+1] = 255
				captchaImage.Pix[pixelIndex+2] = 255
				captchaImage.Pix[pixelIndex+3] = 255
			}
			frameRandom := captchaImageRandom{}
			for index := range frameRandom.values {
				frameRandom.values[index] = byte((index*31 + frameIndex*19 + 5) % 256)
			}
			if err := drawLoginCaptchaText(captchaImage, code, &frameRandom, frameIndex, loginCaptchaAnimationMaxFrameCount, 0); err != nil {
				t.Fatalf("drawLoginCaptchaText(%q frame=%d) error = %v", code, frameIndex, err)
			}
			bounds := captchaImage.Bounds()
			textPixels := 0
			for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
				for x := bounds.Min.X; x < bounds.Max.X; x++ {
					pixel := captchaImage.NRGBAAt(x, y)
					if pixel.R == 255 && pixel.G == 255 && pixel.B == 255 {
						continue
					}
					textPixels++
					if x < bounds.Min.X+loginCaptchaCharacterPadding || x >= bounds.Max.X-loginCaptchaCharacterPadding || y < bounds.Min.Y+loginCaptchaCharacterPadding || y >= bounds.Max.Y-loginCaptchaCharacterPadding {
						t.Fatalf("验证码 %q 深色文字像素贴边: x=%d y=%d bounds=%v", code, x, y, bounds)
					}
				}
			}
			if textPixels == 0 {
				t.Fatalf("验证码 %q 第 %d 帧未绘制主字符", code, frameIndex)
			}
		}
	}
}

// TestGenerateLoginCaptchaCodeCoversRequestedGroups 验证主验证码只使用安全基础字符，并能随机显示大写、小写和数字。
func TestGenerateLoginCaptchaCodeCoversRequestedGroups(t *testing.T) {
	allowed := make(map[rune]struct{}, len(loginCaptchaAlphabet))
	for _, character := range loginCaptchaAlphabet {
		allowed[character] = struct{}{}
	}
	containsLower, containsUpper, containsDigit := false, false, false
	for range 256 {
		code, err := generateLoginCaptchaCode(loginCaptchaLength)
		if err != nil {
			t.Fatalf("generateLoginCaptchaCode() error = %v", err)
		}
		if len([]rune(code)) != loginCaptchaLength {
			t.Fatalf("generateLoginCaptchaCode() length = %d, want %d", len([]rune(code)), loginCaptchaLength)
		}
		for _, character := range code {
			if _, ok := allowed[unicode.ToUpper(character)]; !ok {
				t.Fatalf("主验证码包含基础字符集之外的字符 %q", character)
			}
			containsLower = containsLower || unicode.IsLower(character)
			containsUpper = containsUpper || unicode.IsUpper(character)
			containsDigit = containsDigit || unicode.IsDigit(character)
		}
	}
	if !containsLower || !containsUpper || !containsDigit {
		t.Fatalf("主验证码随机分类不完整: lower=%t upper=%t digit=%t", containsLower, containsUpper, containsDigit)
	}
}

// TestLoginCaptchaBackgroundCharacterSetCoversRequestedGroups 验证背景固定绘制 12 个干扰字符，且字符集包含小写字母、大写字母和数字。
func TestLoginCaptchaBackgroundCharacterSetCoversRequestedGroups(t *testing.T) {
	if loginCaptchaBackgroundCharacterCount != 12 {
		t.Fatalf("背景干扰字符数量=%d，期望 12", loginCaptchaBackgroundCharacterCount)
	}
	if loginCaptchaBackgroundCharacterMinFontSize != 18 || loginCaptchaBackgroundCharacterMaxFontSize != 24 {
		t.Fatalf("背景干扰字符字号范围=%d-%d，期望 18-24pt", loginCaptchaBackgroundCharacterMinFontSize, loginCaptchaBackgroundCharacterMaxFontSize)
	}
	if loginCaptchaGuideLineCount != 3 {
		t.Fatalf("背景波浪引导线数量=%d，期望 3", loginCaptchaGuideLineCount)
	}
	if loginCaptchaBottomSnowflakeCount != 3 || loginCaptchaBottomStarCount != 3 {
		t.Fatalf("最底层图案数量 snowflake=%d star=%d，期望各 3 个", loginCaptchaBottomSnowflakeCount, loginCaptchaBottomStarCount)
	}
	containsLower, containsUpper, containsDigit := false, false, false
	for _, character := range loginCaptchaBackgroundCharacterSet {
		containsLower = containsLower || unicode.IsLower(character)
		containsUpper = containsUpper || unicode.IsUpper(character)
		containsDigit = containsDigit || unicode.IsDigit(character)
	}
	if !containsLower || !containsUpper || !containsDigit {
		t.Fatalf("背景字符集分类不完整: lower=%t upper=%t digit=%t", containsLower, containsUpper, containsDigit)
	}
}

// TestLoginCaptchaBackgroundPositionsAvoidOverlap 验证 12 个 18-24pt 变化字符优先选择互不重叠的位置。
func TestLoginCaptchaBackgroundPositionsAvoidOverlap(t *testing.T) {
	imageBounds := image.Rect(0, 0, loginCaptchaImageWidth, loginCaptchaImageHeight)
	occupied := make([]image.Rectangle, 0, loginCaptchaBackgroundCharacterCount)
	for index := range loginCaptchaBackgroundCharacterCount {
		fontSize := loginCaptchaBackgroundCharacterMinFontSize + index%(loginCaptchaBackgroundCharacterMaxFontSize-loginCaptchaBackgroundCharacterMinFontSize+1)
		face := truetype.NewFace(loginCaptchaFont, &truetype.Options{
			DPI:     loginCaptchaDPI,
			Hinting: font.HintingFull,
			Size:    float64(fontSize),
		})
		drawer := font.Drawer{Face: face}
		_, _, inkBounds := loginCaptchaBackgroundPosition(
			&drawer,
			"8",
			index,
			0.271,
			0.137,
			imageBounds,
			occupied,
		)
		_ = face.Close()
		if overlap := loginCaptchaBackgroundOverlapArea(inkBounds, occupied); overlap != 0 {
			t.Fatalf("背景字符 %d 字号=%d 与既有字符重叠面积=%d bounds=%v occupied=%v", index, fontSize, overlap, inkBounds, occupied)
		}
		occupied = append(occupied, inkBounds)
	}
}

// TestLoginCaptchaBackgroundFontSizeCoversConfiguredRange 验证背景字号随机值完整覆盖 18-24pt，且不会越过人工识别层级上限。
func TestLoginCaptchaBackgroundFontSizeCoversConfiguredRange(t *testing.T) {
	random := captchaImageRandom{}
	for index := 0; index <= loginCaptchaBackgroundCharacterMaxFontSize-loginCaptchaBackgroundCharacterMinFontSize; index++ {
		random.values[index] = byte(index)
		fontSize := loginCaptchaBackgroundCharacterFontSize(&random)
		expected := loginCaptchaBackgroundCharacterMinFontSize + index
		if fontSize != expected {
			t.Fatalf("背景字号第 %d 个值=%d，期望 %d", index, fontSize, expected)
		}
	}
}

// TestDrawLoginCaptchaGradientBackgroundUsesTwoLightColors 验证静态底图存在颜色渐变，并保持浅色以免压过主验证码。
func TestDrawLoginCaptchaGradientBackgroundUsesTwoLightColors(t *testing.T) {
	canvas := image.NewNRGBA(image.Rect(0, 0, loginCaptchaImageWidth, loginCaptchaImageHeight))
	random := captchaImageRandom{}
	drawLoginCaptchaGradientBackground(canvas, &random)
	start := canvas.NRGBAAt(0, loginCaptchaImageHeight/2)
	end := canvas.NRGBAAt(loginCaptchaImageWidth-1, loginCaptchaImageHeight/2)
	if start == end {
		t.Fatalf("渐变背景首尾颜色相同: start=%#v end=%#v", start, end)
	}
	for _, pixel := range []color.NRGBA{start, end, canvas.NRGBAAt(loginCaptchaImageWidth/2, loginCaptchaImageHeight/2)} {
		if int(pixel.R)+int(pixel.G)+int(pixel.B) < 700 {
			t.Fatalf("渐变背景颜色过深: %#v", pixel)
		}
	}
}

// TestDrawLoginCaptchaGuideWaveHasPeaksAndValleys 验证背景导线使用正弦波轨迹而不是水平线或斜直线。
func TestDrawLoginCaptchaGuideWaveHasPeaksAndValleys(t *testing.T) {
	canvas := image.NewNRGBA(image.Rect(0, 0, loginCaptchaImageWidth, loginCaptchaImageHeight))
	lineColor := color.RGBA{R: 1, G: 2, B: 3, A: 255}
	const (
		baseY      = 22
		amplitude  = 4
		wavelength = 32
	)
	drawLoginCaptchaGuideWave(canvas, baseY, amplitude, wavelength, 0, lineColor)
	for _, point := range []image.Point{
		{X: 0, Y: baseY},
		{X: wavelength / 4, Y: baseY + amplitude},
		{X: wavelength / 2, Y: baseY},
		{X: wavelength * 3 / 4, Y: baseY - amplitude},
	} {
		if actual := canvas.NRGBAAt(point.X, point.Y); actual != color.NRGBAModel.Convert(lineColor).(color.NRGBA) {
			t.Fatalf("正弦导线关键点 %v 像素=%#v，期望 %#v", point, actual, lineColor)
		}
	}
}

// TestDrawLoginCaptchaBackgroundCharactersStayVisibleAndScattered 验证背景字符保持可见的中浅色并横纵分散，不与主验证码争夺视觉层级。
func TestDrawLoginCaptchaBackgroundCharactersStayVisibleAndScattered(t *testing.T) {
	background := color.NRGBA{R: 255, G: 255, B: 255, A: 255}
	canvas := image.NewNRGBA(image.Rect(0, 0, loginCaptchaImageWidth, loginCaptchaImageHeight))
	for y := 0; y < loginCaptchaImageHeight; y++ {
		for x := 0; x < loginCaptchaImageWidth; x++ {
			canvas.SetNRGBA(x, y, background)
		}
	}
	random := captchaImageRandom{}
	for index := range random.values {
		random.values[index] = byte((index*37 + 11) % 256)
	}
	if err := drawLoginCaptchaBackgroundCharacters(canvas, &random); err != nil {
		t.Fatalf("drawLoginCaptchaBackgroundCharacters() error = %v", err)
	}

	minX, minY := loginCaptchaImageWidth, loginCaptchaImageHeight
	maxX, maxY := 0, 0
	changedPixels := 0
	darkestPixelBrightness := 255 * 3
	for y := 0; y < loginCaptchaImageHeight; y++ {
		for x := 0; x < loginCaptchaImageWidth; x++ {
			pixel := canvas.NRGBAAt(x, y)
			if pixel == background {
				continue
			}
			changedPixels++
			minX, maxX = min(minX, x), max(maxX, x)
			minY, maxY = min(minY, y), max(maxY, y)
			brightness := int(pixel.R) + int(pixel.G) + int(pixel.B)
			darkestPixelBrightness = min(darkestPixelBrightness, brightness)
			if brightness < 500 {
				t.Fatalf("背景字符像素过深: x=%d y=%d color=%#v", x, y, pixel)
			}
		}
	}
	if changedPixels == 0 {
		t.Fatal("背景干扰字符未绘制任何像素")
	}
	if darkestPixelBrightness > 570 {
		t.Fatalf("背景字符与浅色底图对比不足: darkest_brightness=%d", darkestPixelBrightness)
	}
	if maxX-minX < loginCaptchaImageWidth/2 || maxY-minY < loginCaptchaImageHeight/3 {
		t.Fatalf("背景字符分布过于集中: x=%d..%d y=%d..%d", minX, maxX, minY, maxY)
	}
	if minX > loginCaptchaImageWidth/10 || maxX < loginCaptchaImageWidth*9/10 || minY > 10 || maxY < loginCaptchaImageHeight-10 {
		t.Fatalf("背景字符未覆盖图片边缘区域: x=%d..%d y=%d..%d", minX, maxX, minY, maxY)
	}
}

// TestLoginCaptchaBackgroundPositionAllowsTouchingImageEdge 验证背景字符不再预留内边距，候选点可以贴到图片左边界。
func TestLoginCaptchaBackgroundPositionAllowsTouchingImageEdge(t *testing.T) {
	face := truetype.NewFace(loginCaptchaFont, &truetype.Options{
		DPI:     loginCaptchaDPI,
		Hinting: font.HintingFull,
		Size:    loginCaptchaBackgroundCharacterMaxFontSize,
	})
	defer face.Close()
	drawer := font.Drawer{Face: face}
	imageBounds := image.Rect(0, 0, loginCaptchaImageWidth, loginCaptchaImageHeight)
	_, _, inkBounds := loginCaptchaBackgroundPosition(
		&drawer,
		"W",
		0,
		0,
		0.137,
		imageBounds,
		nil,
	)
	if inkBounds.Min.X != imageBounds.Min.X {
		t.Fatalf("背景字符左边界=%d，期望允许贴到图片左边界 %d", inkBounds.Min.X, imageBounds.Min.X)
	}
	if !inkBounds.In(imageBounds) {
		t.Fatalf("背景字符边界=%v，期望仍位于图片范围 %v 内", inkBounds, imageBounds)
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
	drawLoginCaptchaRainbowBand(canvas, left, frameIndex, bandIndex, loginCaptchaRainbowPrimaryAlpha, loginCaptchaAnimationMinFrameCount)
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
	drawLoginCaptchaRainbowBand(revealed, -loginCaptchaImageWidth, frameIndex, bandIndex, loginCaptchaRainbowPrimaryAlpha, loginCaptchaAnimationMinFrameCount)
	if actual := revealed.NRGBAAt(startX, y); actual != background {
		t.Fatalf("波浪带移开后像素=%#v，期望恢复原始背景 %#v", actual, background)
	}
}

// TestDrawLoginCaptchaHorizontalRainbowLinesMove 验证 3 条横向彩虹线纵向错开、逐帧移动且使用完全不透明像素。
func TestDrawLoginCaptchaHorizontalRainbowLinesMove(t *testing.T) {
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
	const frameCount = 8
	drawLoginCaptchaHorizontalRainbowLines(first, 0, frameCount)
	drawLoginCaptchaHorizontalRainbowLines(second, frameCount/4, frameCount)
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
	coloredRows := map[int]struct{}{}
	for y := 0; y < loginCaptchaImageHeight; y++ {
		pixel := first.NRGBAAt(loginCaptchaImageWidth/2, y)
		if pixel.R != 255 || pixel.G != 255 || pixel.B != 255 {
			coloredRows[y] = struct{}{}
		}
	}
	if len(coloredRows) < loginCaptchaHorizontalRainbowLineCount {
		t.Fatalf("图片中心仅覆盖 %d 个彩色行，期望至少对应 %d 条横向线", len(coloredRows), loginCaptchaHorizontalRainbowLineCount)
	}
}

// TestLoginCaptchaAnimationPlanVariesTimingAndKeepsCharactersVisible 验证任意帧都不暴露完整答案，同时保留人工识别时段。
func TestLoginCaptchaAnimationPlanVariesTimingAndKeepsCharactersVisible(t *testing.T) {
	random := captchaImageRandom{}
	for index := range random.values {
		random.values[index] = byte((index*43 + 17) % 256)
	}
	plan := newLoginCaptchaAnimationPlan(&random, loginCaptchaLength)
	if plan.frameCount < loginCaptchaAnimationMinFrameCount || plan.frameCount > loginCaptchaAnimationMaxFrameCount {
		t.Fatalf("动画帧数=%d，期望范围 %d-%d", plan.frameCount, loginCaptchaAnimationMinFrameCount, loginCaptchaAnimationMaxFrameCount)
	}
	if len(plan.delays) != plan.frameCount || len(plan.hiddenMasks) != plan.frameCount {
		t.Fatalf("动画计划长度 frame=%d delay=%d mask=%d", plan.frameCount, len(plan.delays), len(plan.hiddenMasks))
	}
	for frameIndex, delay := range plan.delays {
		if delay < loginCaptchaMinFrameDelayCentiseconds || delay > loginCaptchaMaxFrameDelayCentiseconds {
			t.Fatalf("第 %d 帧延迟=%d，期望范围 %d-%d", frameIndex, delay, loginCaptchaMinFrameDelayCentiseconds, loginCaptchaMaxFrameDelayCentiseconds)
		}
		if hiddenCount := countSetBits(plan.hiddenMasks[frameIndex]); hiddenCount < 1 || hiddenCount > 2 {
			t.Fatalf("第 %d 帧隐藏字符数=%d，必须为 1-2", frameIndex, hiddenCount)
		}
	}
	minimumVisibleFrames := (plan.frameCount + 1) / 2
	for characterIndex := range loginCaptchaLength {
		visibleFrames := 0
		for _, hiddenMask := range plan.hiddenMasks {
			if hiddenMask&(uint64(1)<<characterIndex) == 0 {
				visibleFrames++
			}
		}
		if visibleFrames < minimumVisibleFrames {
			t.Fatalf("字符 %d 可见帧数=%d，至少应为 %d", characterIndex, visibleFrames, minimumVisibleFrames)
		}
	}
}

// TestLoginCaptchaAnimationPlanNeverExposesFullAnswer 验证不同随机序列生成的每一帧都至少隐藏一个主字符，防止单帧截图取得完整答案。
func TestLoginCaptchaAnimationPlanNeverExposesFullAnswer(t *testing.T) {
	for seed := range 256 {
		random := captchaImageRandom{}
		for index := range random.values {
			random.values[index] = byte(seed + index*43)
		}
		plan := newLoginCaptchaAnimationPlan(&random, loginCaptchaLength)
		for frameIndex, hiddenMask := range plan.hiddenMasks {
			if hiddenCount := countSetBits(hiddenMask); hiddenCount < 1 || hiddenCount > 2 {
				t.Fatalf("随机序列=%d 第 %d 帧隐藏字符数=%d，必须为 1-2", seed, frameIndex, hiddenCount)
			}
		}
	}
}

// TestLoginCaptchaCharactersHideJumpAndTilt 验证随机掩码只隐藏目标字符，且不同帧的位置和偏转确实变化。
func TestLoginCaptchaCharactersHideJumpAndTilt(t *testing.T) {
	const code = "1234"
	random := captchaImageRandom{}
	for index := range random.values {
		random.values[index] = byte((index*29 + 7) % 256)
	}
	const frameCount = 8
	render := func(frameIndex int, hiddenMask uint64) *image.NRGBA {
		canvas := image.NewNRGBA(image.Rect(0, 0, loginCaptchaImageWidth, loginCaptchaImageHeight))
		for y := 0; y < loginCaptchaImageHeight; y++ {
			for x := 0; x < loginCaptchaImageWidth; x++ {
				canvas.SetNRGBA(x, y, color.NRGBA{R: 255, G: 255, B: 255, A: 255})
			}
		}
		frameRandom := random
		frameRandom.index += frameIndex * loginCaptchaFrameRandomStride
		if err := drawLoginCaptchaText(canvas, code, &frameRandom, frameIndex, frameCount, hiddenMask); err != nil {
			t.Fatalf("drawLoginCaptchaText(frame=%d mask=%b) error = %v", frameIndex, hiddenMask, err)
		}
		return canvas
	}
	safeWidth := loginCaptchaImageWidth - loginCaptchaCharacterPadding*2
	for frameIndex := range frameCount {
		hiddenIndex := (frameIndex*3 + 1) % len([]rune(code))
		allVisible := render(frameIndex, 0)
		oneHidden := render(frameIndex, uint64(1)<<hiddenIndex)
		for characterIndex := range len([]rune(code)) {
			startX := loginCaptchaCharacterPadding + characterIndex*safeWidth/len([]rune(code))
			endX := loginCaptchaCharacterPadding + (characterIndex+1)*safeWidth/len([]rune(code))
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
	if bytes.Equal(render(0, 0).Pix, render(frameCount/4, 0).Pix) {
		t.Fatal("字符在四分之一周期后必须改变纵向位置或偏转角")
	}
}

// TestLoginCaptchaCharacterTiltStaysBounded 验证随机基础偏角叠加逐帧摆动后不超过正负十三度，且相邻阶段角度确实变化。
func TestLoginCaptchaCharacterTiltStaysBounded(t *testing.T) {
	const frameCount = 9
	for _, baseTilt := range []int{-loginCaptchaCharacterBaseTiltDegrees, loginCaptchaCharacterBaseTiltDegrees} {
		for characterIndex := range loginCaptchaLength {
			previous := loginCaptchaCharacterTilt(baseTilt, 0, characterIndex, frameCount)
			changed := false
			for frameIndex := 1; frameIndex < frameCount; frameIndex++ {
				angle := loginCaptchaCharacterTilt(baseTilt, frameIndex, characterIndex, frameCount)
				if math.Abs(angle) > loginCaptchaCharacterBaseTiltDegrees+loginCaptchaCharacterTiltSwingDegrees+0.0001 {
					t.Fatalf("基础偏角 %d 字符 %d 第 %d 帧偏角=%f，超过允许上限", baseTilt, characterIndex, frameIndex, angle)
				}
				if math.Abs(angle-previous) > 0.0001 {
					changed = true
				}
				previous = angle
			}
			if !changed {
				t.Fatalf("基础偏角 %d 字符 %d 的逐帧偏角未变化", baseTilt, characterIndex)
			}
		}
	}
}

// countSetBits 统计遮挡掩码中的字符数量，测试只关心低四位的主验证码字符。
func countSetBits(value uint64) int {
	count := 0
	for value != 0 {
		count += int(value & 1)
		value >>= 1
	}
	return count
}

// BenchmarkDrawLoginCaptchaGIF 记录随机帧数方案的生成耗时和平均 GIF 大小，防止视觉增强造成无界成本。
func BenchmarkDrawLoginCaptchaGIF(b *testing.B) {
	totalBytes := 0
	for range b.N {
		imageBytes, err := drawLoginCaptchaGIF("aB7W")
		if err != nil {
			b.Fatalf("drawLoginCaptchaGIF() error = %v", err)
		}
		totalBytes += len(imageBytes)
	}
	if b.N > 0 {
		b.ReportMetric(float64(totalBytes)/float64(b.N), "bytes/gif")
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
