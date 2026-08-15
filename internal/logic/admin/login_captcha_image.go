package admin

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/gif"
	"math"
	"strings"

	"github.com/Is999/go-utils/errors"
	"github.com/golang/freetype"
	"github.com/golang/freetype/truetype"
	base64Captcha "github.com/mojocn/base64Captcha"
	"golang.org/x/image/font"
)

const (
	// loginCaptchaImageHeight 表示登录图形验证码图片高度。
	loginCaptchaImageHeight = 44
	// loginCaptchaImageWidth 表示登录图形验证码图片宽度。
	loginCaptchaImageWidth = 120
	// loginCaptchaFontSize 表示登录图形验证码字体大小。
	loginCaptchaFontSize = 31
	// loginCaptchaCharacterPadding 表示主字符与图片四边的最小像素距离；旋转和跳动后仍保留 3px 空白，避免笔画裁切。
	loginCaptchaCharacterPadding = 3
	// loginCaptchaGuideLineCount 表示每张验证码绘制 3 条背景淡色波浪线；线条先于文字绘制，避免遮挡字符。
	loginCaptchaGuideLineCount = 3
	// loginCaptchaBottomSnowflakeCount 表示渐变底图上绘制 3 个浅色雪花，位于引导线和所有字符下方。
	loginCaptchaBottomSnowflakeCount = 3
	// loginCaptchaBottomStarCount 表示渐变底图上绘制 3 个浅色五星，位于引导线和所有字符下方。
	loginCaptchaBottomStarCount = 3
	// loginCaptchaBackgroundCharacterCount 表示共享背景固定绘制 12 个大小写字母或数字，字符不参与验证码答案。
	loginCaptchaBackgroundCharacterCount = 12
	// loginCaptchaBackgroundCharacterMinFontSize 表示背景字符最小字号，单位 point；小于主字符以保留人工识别层级。
	loginCaptchaBackgroundCharacterMinFontSize = 18
	// loginCaptchaBackgroundCharacterMaxFontSize 表示背景字符最大字号，单位 point；均匀随机后每张图仅少量字符接近 24pt。
	loginCaptchaBackgroundCharacterMaxFontSize = 24
	// loginCaptchaBackgroundCharacterVerticalJitter 表示低差异纵向基线叠加的随机偏移上限，单位像素。
	loginCaptchaBackgroundCharacterVerticalJitter = 1
	// loginCaptchaBackgroundPlacementAttempts 表示每个背景字符最多评估 64 个候选点；最宽字形仍需优先找到不重叠位置。
	loginCaptchaBackgroundPlacementAttempts = 64
	// loginCaptchaForegroundCurveCount 表示文字上方绘制 2 条彩色波浪曲线，用单像素交叉轨迹增加字符分割难度。
	loginCaptchaForegroundCurveCount = 2
	// loginCaptchaAnimationMinFrameCount 表示单张动态验证码最少 6 帧，避免动画过短导致字符没有足够可见时段。
	loginCaptchaAnimationMinFrameCount = 6
	// loginCaptchaAnimationMaxFrameCount 表示单张动态验证码最多 10 帧，限制 GIF 体积和接口渲染耗时。
	loginCaptchaAnimationMaxFrameCount = 10
	// loginCaptchaMinFrameDelayCentiseconds 表示 GIF 单帧最短停留 14 厘秒，即 140 毫秒。
	loginCaptchaMinFrameDelayCentiseconds = 14
	// loginCaptchaMaxFrameDelayCentiseconds 表示 GIF 单帧最长停留 22 厘秒，即 220 毫秒。
	loginCaptchaMaxFrameDelayCentiseconds = 22
	// loginCaptchaRainbowBandCount 表示动态图片内同时移动 2 条彩虹波浪带。
	loginCaptchaRainbowBandCount = 2
	// loginCaptchaRainbowBandWidth 表示单条波浪带占图片宽度的 30%。
	loginCaptchaRainbowBandWidth = loginCaptchaImageWidth * 3 / 10
	// loginCaptchaRainbowWaveAmplitude 表示波浪带左右边缘的最大横向振幅，单位像素。
	loginCaptchaRainbowWaveAmplitude = 2
	// loginCaptchaRainbowSolidLineCount 表示每条波浪带内部绘制 3 条完全不透明的彩色线，扫过时短暂遮挡字符像素。
	loginCaptchaRainbowSolidLineCount = 3
	// loginCaptchaHorizontalWaveAmplitude 表示横向彩虹线相对中心轨迹的上下振幅，单位像素。
	loginCaptchaHorizontalWaveAmplitude = 3
	// loginCaptchaHorizontalRainbowLineCount 表示动态图片内同时绘制 3 条横向彩虹波浪线。
	loginCaptchaHorizontalRainbowLineCount = 3
	// loginCaptchaHorizontalRainbowLineSpacing 表示 3 条横向彩虹线的中心轨迹间距，单位像素。
	loginCaptchaHorizontalRainbowLineSpacing = 7
	// loginCaptchaHorizontalSweepAmplitude 表示横向彩虹线逐帧上下摆动的最大距离，单位像素。
	loginCaptchaHorizontalSweepAmplitude = 8
	// loginCaptchaCharacterJumpAmplitude 表示每个字符逐帧上下跳动的最大距离，单位像素；5px 振幅扩大字符错位但仍受 3px 边距约束。
	loginCaptchaCharacterJumpAmplitude = 5
	// loginCaptchaCharacterOffsetX 表示主字符每帧水平随机错位上限，单位像素。
	loginCaptchaCharacterOffsetX = 4
	// loginCaptchaCharacterOffsetY 表示主字符每帧纵向随机错位上限，单位像素。
	loginCaptchaCharacterOffsetY = 3
	// loginCaptchaCharacterBaseTiltDegrees 表示每个主字符每帧随机基础偏角上限，单位度，实际范围为 -9 至 +9。
	loginCaptchaCharacterBaseTiltDegrees = 9
	// loginCaptchaCharacterTiltSwingDegrees 表示主字符逐帧摆动的角度振幅，单位度；与随机偏角合成后不超过正负 13 度。
	loginCaptchaCharacterTiltSwingDegrees = 4
	// loginCaptchaCharacterGlyphPadding 表示旋转字符槽位的水平透明留白，单位像素，避免小角度偏转裁切笔画。
	loginCaptchaCharacterGlyphPadding = 5
	// loginCaptchaRainbowSlantDegrees 表示波浪带相对垂直方向向右倾斜的角度。
	loginCaptchaRainbowSlantDegrees = 14
	// loginCaptchaRainbowPrimaryAlpha 表示主波浪带约 36% 的不透明度；带内实体线仍保持完全不透明。
	loginCaptchaRainbowPrimaryAlpha = 92
	// loginCaptchaRainbowSecondaryAlpha 表示次波浪带约 28% 的不透明度；带内实体线仍保持完全不透明。
	loginCaptchaRainbowSecondaryAlpha = 72
	// loginCaptchaDPI 表示验证码字体渲染 DPI。
	loginCaptchaDPI = 72
	// loginCaptchaMimeType 表示验证码图片 MIME 类型。
	loginCaptchaMimeType = "image/gif"
	// loginCaptchaFrameRandomStride 表示相邻帧文字随机起点的字节跨度，避免字符颜色、位置和偏角重复同一模板。
	loginCaptchaFrameRandomStride = 29
	// loginCaptchaRandomBytes 表示单张图片预读的随机字节数；512 字节覆盖背景、动画计划和最多 10 帧的文字错位且不回绕。
	loginCaptchaRandomBytes = 512
	// loginCaptchaPaletteLookupBits 表示调色板查找表为每个 RGB 通道保留 5 bit，查找表固定占用 32KB。
	loginCaptchaPaletteLookupBits = 5
	// loginCaptchaPaletteLookupSize 表示 5-bit RGB 组合总数。
	loginCaptchaPaletteLookupSize = 1 << (loginCaptchaPaletteLookupBits * 3)
)

// captchaImageRandom 保存单张图片的随机数据，避免每个噪声点单独读取系统随机源。
type captchaImageRandom struct {
	values [loginCaptchaRandomBytes]byte // values 保存本张图片使用的随机字节
	index  int                           // index 表示下一个待读取位置
}

// captchaAnimationPlan 保存单张验证码的帧数、逐帧延迟和遮挡掩码，阻止不同请求复用固定动画时序。
type captchaAnimationPlan struct {
	frameCount  int      // frameCount 是本张 GIF 的总帧数，范围为 6-10
	delays      []int    // delays 按帧保存停留时间，单位为 GIF 厘秒
	hiddenMasks []uint64 // hiddenMasks 的 bit 位置对应主字符下标，置位表示该帧隐藏该字符
}

// loginCaptchaFont 使用稳定字体渲染验证码，避免随机花体导致字符裁切或难辨认。
var loginCaptchaFont = base64Captcha.DefaultEmbeddedFonts.LoadFontByName("fonts/wqy-microhei.ttc")

// loginCaptchaBackgroundColors 定义登录验证码的浅色背景池。
var loginCaptchaBackgroundColors = []color.RGBA{
	{R: 244, G: 240, B: 255, A: 255},
	{R: 239, G: 246, B: 255, A: 255},
	{R: 255, G: 242, B: 246, A: 255},
	{R: 240, G: 253, B: 244, A: 255},
}

// loginCaptchaGuideLineColors 定义登录验证码的淡背景线颜色池。
var loginCaptchaGuideLineColors = []color.RGBA{
	{R: 184, G: 203, B: 226, A: 255},
	{R: 203, G: 190, B: 229, A: 255},
	{R: 219, G: 191, B: 209, A: 255},
	{R: 188, G: 215, B: 198, A: 255},
}

// loginCaptchaBackgroundCharacterSet 定义背景噪声使用的大小写英文字母和数字；这些字符只增加分割干扰，不进入 Redis 验证答案。
var loginCaptchaBackgroundCharacterSet = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// loginCaptchaBackgroundCharacterColors 定义背景字符中浅色池；颜色与浅色底图保持可见对比，同时弱于主验证码的深色字形。
var loginCaptchaBackgroundCharacterColors = []color.RGBA{
	{R: 150, G: 174, B: 201, A: 255},
	{R: 174, G: 151, B: 201, A: 255},
	{R: 201, G: 157, B: 179, A: 255},
	{R: 151, G: 189, B: 168, A: 255},
	{R: 198, G: 182, B: 137, A: 255},
	{R: 153, G: 184, B: 191, A: 255},
}

// loginCaptchaForegroundCurveColors 定义前景波浪曲线颜色池；颜色保持明亮，避免单像素曲线遮蔽字符主体。
var loginCaptchaForegroundCurveColors = []color.RGBA{
	{R: 242, G: 132, B: 164, A: 255},
	{R: 244, G: 171, B: 104, A: 255},
	{R: 218, G: 190, B: 92, A: 255},
	{R: 108, G: 196, B: 147, A: 255},
	{R: 94, G: 188, B: 207, A: 255},
	{R: 137, G: 158, B: 224, A: 255},
	{R: 185, G: 139, B: 221, A: 255},
}

// loginCaptchaRainbowColors 定义动态波浪带的 11 段彩虹色，逐帧偏移色段使每次扫过的配色不同。
var loginCaptchaRainbowColors = []color.RGBA{
	{R: 255, G: 45, B: 149, A: 255},
	{R: 255, G: 61, B: 90, A: 255},
	{R: 255, G: 122, B: 24, A: 255},
	{R: 255, G: 201, B: 40, A: 255},
	{R: 216, G: 242, B: 56, A: 255},
	{R: 52, G: 211, B: 153, A: 255},
	{R: 32, G: 227, B: 178, A: 255},
	{R: 34, G: 211, B: 238, A: 255},
	{R: 59, G: 130, B: 246, A: 255},
	{R: 99, G: 102, B: 241, A: 255},
	{R: 168, G: 85, B: 247, A: 255},
}

// loginCaptchaTextColors 定义登录验证码文字颜色池，保证深色文字覆盖淡背景线。
var loginCaptchaTextColors = []color.RGBA{
	{R: 37, G: 99, B: 235, A: 255},
	{R: 124, G: 58, B: 237, A: 255},
	{R: 220, G: 38, B: 38, A: 255},
	{R: 5, G: 150, B: 105, A: 255},
	{R: 217, G: 119, B: 6, A: 255},
	{R: 8, G: 145, B: 178, A: 255},
	{R: 225, G: 29, B: 72, A: 255},
	{R: 79, G: 70, B: 229, A: 255},
}

// loginCaptchaGIFPalette 收口验证码已知颜色及彩虹中间色；固定小调色板配合无抖动量化，减少逐帧转换时间和 GIF 噪声体积。
var loginCaptchaGIFPalette = buildLoginCaptchaGIFPalette()

// loginCaptchaPaletteLookup 保存 5-bit RGB 到固定 GIF 调色板索引的最近色映射，避免每帧每像素线性扫描调色板。
var loginCaptchaPaletteLookup = buildLoginCaptchaPaletteLookup()

// buildLoginCaptchaGIFPalette 从验证码现有颜色池构建共享调色板，避免维护第二套与绘制颜色漂移的枚举。
func buildLoginCaptchaGIFPalette() color.Palette {
	colorGroups := [][]color.RGBA{
		loginCaptchaBackgroundColors,
		loginCaptchaGuideLineColors,
		loginCaptchaBackgroundCharacterColors,
		loginCaptchaForegroundCurveColors,
		loginCaptchaRainbowColors,
		loginCaptchaTextColors,
	}
	result := make(color.Palette, 0, 160)
	for _, colors := range colorGroups {
		for _, item := range colors {
			result = append(result, item)
		}
	}
	for index, current := range loginCaptchaRainbowColors {
		next := loginCaptchaRainbowColors[(index+1)%len(loginCaptchaRainbowColors)]
		for _, fraction := range []int{64, 128, 192} {
			result = append(result, interpolateLoginCaptchaColor(current, next, fraction))
		}
	}
	for startIndex, start := range loginCaptchaBackgroundColors {
		for endIndex := startIndex + 1; endIndex < len(loginCaptchaBackgroundColors); endIndex++ {
			end := loginCaptchaBackgroundColors[endIndex]
			for _, fraction := range []int{32, 64, 96, 128, 160, 192, 224} {
				result = append(result, interpolateLoginCaptchaColor(start, end, fraction))
			}
		}
	}
	return result
}

// buildLoginCaptchaPaletteLookup 在进程初始化期一次性计算 32KB 最近色表，运行期验证码请求只执行数组索引。
func buildLoginCaptchaPaletteLookup() [loginCaptchaPaletteLookupSize]uint8 {
	result := [loginCaptchaPaletteLookupSize]uint8{}
	channelShift := 8 - loginCaptchaPaletteLookupBits
	channelMask := 1<<loginCaptchaPaletteLookupBits - 1
	halfStep := 1 << (channelShift - 1)
	for key := range loginCaptchaPaletteLookupSize {
		red := uint8(((key>>(loginCaptchaPaletteLookupBits*2))&channelMask)<<channelShift + halfStep)
		green := uint8(((key>>loginCaptchaPaletteLookupBits)&channelMask)<<channelShift + halfStep)
		blue := uint8((key&channelMask)<<channelShift + halfStep)
		result[key] = uint8(loginCaptchaGIFPalette.Index(color.NRGBA{R: red, G: green, B: blue, A: 255}))
	}
	return result
}

// buildLoginCaptchaImageDataURL 把验证码文本渲染成循环 GIF data URL。
func buildLoginCaptchaImageDataURL(code string) (string, error) {
	imageBytes, err := drawLoginCaptchaGIF(code)
	if err != nil {
		return "", errors.Tag(err)
	}
	return fmt.Sprintf(
		"data:%s;base64,%s",
		loginCaptchaMimeType,
		base64.StdEncoding.EncodeToString(imageBytes),
	), nil
}

// drawLoginCaptchaGIF 编码循环 GIF；每张图独立随机帧数、延迟、文字错位和遮挡顺序，避免复用固定时间模板。
func drawLoginCaptchaGIF(code string) ([]byte, error) {
	code = strings.TrimSpace(code)
	staticFrame, textRandom, err := drawLoginCaptchaStaticFrame(code)
	if err != nil {
		return nil, errors.Tag(err)
	}
	runes := []rune(code)
	plan := newLoginCaptchaAnimationPlan(&textRandom, len(runes))
	animation := gif.GIF{
		Image:     make([]*image.Paletted, 0, plan.frameCount),
		Delay:     make([]int, 0, plan.frameCount),
		Disposal:  make([]byte, 0, plan.frameCount),
		LoopCount: 0,
	}
	for frameIndex := range plan.frameCount {
		frame := image.NewNRGBA(staticFrame.Bounds())
		draw.Draw(frame, frame.Bounds(), staticFrame, staticFrame.Bounds().Min, draw.Src)
		frameRandom := textRandom
		frameRandom.index += frameIndex * loginCaptchaFrameRandomStride
		if err = drawLoginCaptchaText(frame, code, &frameRandom, frameIndex, plan.frameCount, plan.hiddenMasks[frameIndex]); err != nil {
			return nil, errors.Tag(err)
		}
		drawLoginCaptchaForegroundCurves(frame, &frameRandom)
		drawLoginCaptchaRainbowBands(frame, frameIndex, plan.frameCount)
		drawLoginCaptchaHorizontalRainbowLines(frame, frameIndex, plan.frameCount)
		paletted := convertLoginCaptchaPaletted(frame)
		animation.Image = append(animation.Image, paletted)
		animation.Delay = append(animation.Delay, plan.delays[frameIndex])
		animation.Disposal = append(animation.Disposal, gif.DisposalNone)
	}
	buffer := bytes.Buffer{}
	if err = gif.EncodeAll(&buffer, &animation); err != nil {
		return nil, errors.Wrap(err, "编码登录验证码 GIF 失败")
	}
	return buffer.Bytes(), nil
}

// convertLoginCaptchaPaletted 使用预计算的 5-bit RGB 查找表转换 GIF 帧，保留固定调色板并避免通用逐像素最近色扫描。
func convertLoginCaptchaPaletted(frame *image.NRGBA) *image.Paletted {
	paletted := image.NewPaletted(frame.Bounds(), loginCaptchaGIFPalette)
	channelShift := 8 - loginCaptchaPaletteLookupBits
	bounds := frame.Bounds()
	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		sourceOffset := frame.PixOffset(bounds.Min.X, y)
		targetOffset := paletted.PixOffset(bounds.Min.X, y)
		for x := 0; x < bounds.Dx(); x++ {
			pixelOffset := sourceOffset + x*4
			key := int(frame.Pix[pixelOffset]>>channelShift)<<(loginCaptchaPaletteLookupBits*2) |
				int(frame.Pix[pixelOffset+1]>>channelShift)<<loginCaptchaPaletteLookupBits |
				int(frame.Pix[pixelOffset+2]>>channelShift)
			paletted.Pix[targetOffset+x] = loginCaptchaPaletteLookup[key]
		}
	}
	return paletted
}

// drawLoginCaptchaStaticFrame 绘制各帧共享的背景，返回相同的文字随机起点以稳定字符颜色和基础偏移。
func drawLoginCaptchaStaticFrame(code string) (*image.NRGBA, captchaImageRandom, error) {
	code = strings.TrimSpace(code)
	if code == "" {
		return nil, captchaImageRandom{}, errors.New("验证码内容不能为空")
	}
	random := &captchaImageRandom{}
	if _, err := rand.Read(random.values[:]); err != nil {
		return nil, captchaImageRandom{}, errors.Wrap(err, "读取登录验证码图片随机数据失败")
	}
	canvas := image.NewNRGBA(image.Rect(0, 0, loginCaptchaImageWidth, loginCaptchaImageHeight))
	drawLoginCaptchaGradientBackground(canvas, random)
	drawLoginCaptchaBottomMarks(canvas, random)
	drawLoginCaptchaGuideLines(canvas, random)
	if err := drawLoginCaptchaBackgroundCharacters(canvas, random); err != nil {
		return nil, captchaImageRandom{}, errors.Tag(err)
	}
	return canvas, *random, nil
}

// drawLoginCaptchaGradientBackground 使用两种不同浅色和随机方向填充渐变，保证背景变化不依赖前端 CSS。
func drawLoginCaptchaGradientBackground(canvas *image.NRGBA, random *captchaImageRandom) {
	startIndex := random.intn(len(loginCaptchaBackgroundColors))
	endIndex := random.intn(len(loginCaptchaBackgroundColors) - 1)
	if endIndex >= startIndex {
		endIndex++
	}
	startColor := loginCaptchaBackgroundColors[startIndex]
	endColor := loginCaptchaBackgroundColors[endIndex]
	direction := random.intn(4)
	bounds := canvas.Bounds()
	width := max(1, bounds.Dx()-1)
	height := max(1, bounds.Dy()-1)
	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			relativeX := x - bounds.Min.X
			relativeY := y - bounds.Min.Y
			position, length := relativeX, width
			switch direction {
			case 1:
				position, length = relativeY, height
			case 2:
				position, length = relativeX+relativeY, width+height
			case 3:
				position, length = width-relativeX+relativeY, width+height
			}
			fraction := position * 255 / length
			inverse := 255 - fraction
			pixelIndex := canvas.PixOffset(x, y)
			canvas.Pix[pixelIndex] = uint8((int(startColor.R)*inverse + int(endColor.R)*fraction) / 255)
			canvas.Pix[pixelIndex+1] = uint8((int(startColor.G)*inverse + int(endColor.G)*fraction) / 255)
			canvas.Pix[pixelIndex+2] = uint8((int(startColor.B)*inverse + int(endColor.B)*fraction) / 255)
			canvas.Pix[pixelIndex+3] = 255
		}
	}
}

// drawLoginCaptchaBottomMarks 在渐变底图上随机绘制雪花和五星；调用顺序保证它们位于引导线和所有字符下方。
func drawLoginCaptchaBottomMarks(canvas *image.NRGBA, random *captchaImageRandom) {
	for index := range loginCaptchaBottomSnowflakeCount + loginCaptchaBottomStarCount {
		markColor := random.color(loginCaptchaGuideLineColors)
		radius := 2 + random.intn(3)
		centerX := radius + random.intn(loginCaptchaImageWidth-radius*2)
		centerY := radius + random.intn(loginCaptchaImageHeight-radius*2)
		if index < loginCaptchaBottomSnowflakeCount {
			drawLoginCaptchaSnowflake(canvas, centerX, centerY, radius, markColor)
			continue
		}
		drawLoginCaptchaStar(canvas, centerX, centerY, radius+1, markColor)
	}
}

// drawLoginCaptchaBaseFrame 绘制包含全部字符的静态检查帧，供边界测试确认文字不会被图片裁切。
func drawLoginCaptchaBaseFrame(code string) (*image.NRGBA, error) {
	canvas, textRandom, err := drawLoginCaptchaStaticFrame(code)
	if err != nil {
		return nil, errors.Tag(err)
	}
	if err = drawLoginCaptchaText(canvas, strings.TrimSpace(code), &textRandom, 0, loginCaptchaAnimationMinFrameCount, 0); err != nil {
		return nil, errors.Tag(err)
	}
	drawLoginCaptchaForegroundCurves(canvas, &textRandom)
	return canvas, nil
}

// color 从指定颜色池选择一个颜色。
func (r *captchaImageRandom) color(colors []color.RGBA) color.RGBA {
	return colors[r.intn(len(colors))]
}

// intn 返回 [0, max) 范围内的图片随机数。
func (r *captchaImageRandom) intn(max int) int {
	if max <= 0 {
		return 0
	}
	value := r.values[r.index%len(r.values)]
	r.index++
	return int(value) % max
}

// offset 返回 [-limit, limit] 范围内的图片随机偏移。
func (r *captchaImageRandom) offset(limit int) int {
	return r.intn(limit*2+1) - limit
}

// newLoginCaptchaAnimationPlan 为单张 GIF 生成独立时序；每帧必须隐藏一至两个字符，并保证每个字符至少半数帧可见。
func newLoginCaptchaAnimationPlan(random *captchaImageRandom, characterCount int) captchaAnimationPlan {
	frameCount := loginCaptchaAnimationMinFrameCount + random.intn(loginCaptchaAnimationMaxFrameCount-loginCaptchaAnimationMinFrameCount+1)
	plan := captchaAnimationPlan{
		frameCount:  frameCount,
		delays:      make([]int, frameCount),
		hiddenMasks: make([]uint64, frameCount),
	}
	for frameIndex := range frameCount {
		plan.delays[frameIndex] = loginCaptchaMinFrameDelayCentiseconds + random.intn(loginCaptchaMaxFrameDelayCentiseconds-loginCaptchaMinFrameDelayCentiseconds+1)
	}
	if characterCount <= 0 {
		return plan
	}

	// 首个遮挡位按每轮随机排列分配，避免固定左到右的隐藏模板，也不会让单个字符长期消失。
	hiddenOrder := make([]int, characterCount)
	hiddenCounts := make([]int, characterCount)
	for characterIndex := range characterCount {
		hiddenOrder[characterIndex] = characterIndex
	}
	for frameIndex := range frameCount {
		if frameIndex%characterCount == 0 {
			for index := characterCount - 1; index > 0; index-- {
				swapIndex := random.intn(index + 1)
				hiddenOrder[index], hiddenOrder[swapIndex] = hiddenOrder[swapIndex], hiddenOrder[index]
			}
		}
		characterIndex := hiddenOrder[frameIndex%characterCount]
		plan.hiddenMasks[frameIndex] = uint64(1) << characterIndex
		hiddenCounts[characterIndex]++
	}

	// 少量帧可再隐藏一个字符；上限确保任一主字符仍至少在半数帧中可见。
	maximumHiddenFrames := frameCount / 2
	for frameIndex := range frameCount {
		if characterCount < 2 || random.intn(4) != 0 {
			continue
		}
		startIndex := random.intn(characterCount)
		for offset := range characterCount {
			characterIndex := (startIndex + offset) % characterCount
			bit := uint64(1) << characterIndex
			if plan.hiddenMasks[frameIndex]&bit != 0 || hiddenCounts[characterIndex] >= maximumHiddenFrames {
				continue
			}
			plan.hiddenMasks[frameIndex] |= bit
			hiddenCounts[characterIndex]++
			break
		}
	}
	return plan
}

// drawLoginCaptchaGuideLines 绘制 3 条随机振幅、波长和相位的淡色正弦波，禁止使用斜直线代替波形。
func drawLoginCaptchaGuideLines(canvas *image.NRGBA, random *captchaImageRandom) {
	for range loginCaptchaGuideLineCount {
		lineColor := random.color(loginCaptchaGuideLineColors)
		baseY := 6 + random.intn(loginCaptchaImageHeight-12)
		amplitude := 2 + random.intn(4)
		wavelength := 24 + random.intn(25)
		phase := float64(random.intn(360)) * math.Pi / 180
		drawLoginCaptchaGuideWave(canvas, baseY, amplitude, wavelength, phase, lineColor)
	}
}

// drawLoginCaptchaGuideWave 在图片全宽绘制单条正弦波；振幅必须大于零，保证轨迹出现波峰和波谷。
func drawLoginCaptchaGuideWave(canvas *image.NRGBA, baseY int, amplitude int, wavelength int, phase float64, lineColor color.RGBA) {
	previousY := baseY + int(math.Round(float64(amplitude)*math.Sin(phase)))
	for x := 1; x < loginCaptchaImageWidth; x++ {
		angle := float64(x)*2*math.Pi/float64(wavelength) + phase
		currentY := baseY + int(math.Round(float64(amplitude)*math.Sin(angle)))
		drawLoginCaptchaLine(canvas, x-1, previousY, x, currentY, lineColor)
		previousY = currentY
	}
}

// drawLoginCaptchaBackgroundCharacters 使用 18-24pt 随机字号覆盖图片宽高，并通过字形碰撞检查避免形成固定行或局部重叠。
func drawLoginCaptchaBackgroundCharacters(canvas *image.NRGBA, random *captchaImageRandom) error {
	context := freetype.NewContext()
	context.SetDPI(loginCaptchaDPI)
	context.SetClip(canvas.Bounds())
	context.SetDst(canvas)
	context.SetFont(loginCaptchaFont)
	context.SetHinting(font.HintingFull)
	faces := make(map[int]font.Face, loginCaptchaBackgroundCharacterMaxFontSize-loginCaptchaBackgroundCharacterMinFontSize+1)
	defer func() {
		for _, face := range faces {
			_ = face.Close()
		}
	}()
	bounds := canvas.Bounds()
	horizontalPhase := float64(random.intn(1000)) / 1000
	verticalPhase := float64(random.intn(1000)) / 1000
	occupied := make([]image.Rectangle, 0, loginCaptchaBackgroundCharacterCount)

	for index := range loginCaptchaBackgroundCharacterCount {
		character := string(loginCaptchaBackgroundCharacterSet[random.intn(len(loginCaptchaBackgroundCharacterSet))])
		fontSize := loginCaptchaBackgroundCharacterFontSize(random)
		face := faces[fontSize]
		if face == nil {
			face = truetype.NewFace(loginCaptchaFont, &truetype.Options{
				DPI:     loginCaptchaDPI,
				Hinting: font.HintingFull,
				Size:    float64(fontSize),
			})
			faces[fontSize] = face
		}
		drawer := font.Drawer{Face: face}
		characterColor := random.color(loginCaptchaBackgroundCharacterColors)
		x, y, inkBounds := loginCaptchaBackgroundPosition(
			&drawer,
			character,
			index,
			horizontalPhase,
			verticalPhase,
			bounds,
			occupied,
		)
		occupied = append(occupied, inkBounds)
		context.SetFontSize(float64(fontSize))
		context.SetSrc(image.NewUniform(characterColor))
		if _, err := context.DrawString(character, freetype.Pt(x, y)); err != nil {
			return errors.Wrap(err, "绘制登录验证码背景字符失败")
		}
	}
	return nil
}

// loginCaptchaBackgroundCharacterFontSize 返回 18-24pt 均匀随机字号；12 个字符通常只有少量取到区间上沿。
func loginCaptchaBackgroundCharacterFontSize(random *captchaImageRandom) int {
	return loginCaptchaBackgroundCharacterMinFontSize + random.intn(loginCaptchaBackgroundCharacterMaxFontSize-loginCaptchaBackgroundCharacterMinFontSize+1)
}

// loginCaptchaBackgroundPosition 为单个背景字符选择图片范围内与既有字形重叠面积最小的位置；不预留边距，允许字形贴边。
func loginCaptchaBackgroundPosition(
	drawer *font.Drawer,
	character string,
	index int,
	horizontalPhase float64,
	verticalPhase float64,
	imageBounds image.Rectangle,
	occupied []image.Rectangle,
) (int, int, image.Rectangle) {
	const (
		candidateStep        = 0.4142135623730951
		goldenRatioConjugate = 0.6180339887498949
	)
	glyphBounds, _ := drawer.BoundString(character)
	glyphMinX := glyphBounds.Min.X.Floor()
	glyphMaxX := glyphBounds.Max.X.Ceil()
	glyphMinY := glyphBounds.Min.Y.Floor()
	glyphMaxY := glyphBounds.Max.Y.Ceil()
	minX := imageBounds.Min.X - glyphMinX
	maxX := imageBounds.Max.X - glyphMaxX
	minY := imageBounds.Min.Y - glyphMinY
	maxY := imageBounds.Max.Y - glyphMaxY
	bestOverlap := int(^uint(0) >> 1)
	bestX, bestY := 0, minY
	bestBounds := image.Rectangle{}
	for attempt := range loginCaptchaBackgroundPlacementAttempts {
		// 横向使用二进制反序列、纵向使用黄金比例序列，避免两个坐标按相近步长退化成少量对角轨迹。
		horizontalPosition := math.Mod(horizontalPhase+float64(index)*0.7548776662466927+loginCaptchaBinaryRadicalInverse(attempt), 1)
		x := minX + int(math.Round(horizontalPosition*float64(maxX-minX)))
		candidateIndex := index + attempt
		verticalPosition := math.Mod(verticalPhase+float64(candidateIndex)*goldenRatioConjugate, 1)
		verticalJitter := math.Mod(verticalPhase+float64(candidateIndex)*candidateStep, 1)
		verticalOffset := int(math.Round((verticalJitter*2 - 1) * float64(loginCaptchaBackgroundCharacterVerticalJitter)))
		y := clampInt(minY+int(math.Round(verticalPosition*float64(maxY-minY)))+verticalOffset, minY, maxY)
		inkBounds := image.Rect(x+glyphMinX, y+glyphMinY, x+glyphMaxX, y+glyphMaxY)
		overlap := loginCaptchaBackgroundOverlapArea(inkBounds, occupied)
		if overlap < bestOverlap {
			bestOverlap, bestX, bestY, bestBounds = overlap, x, y, inkBounds
		}
		if overlap == 0 {
			return bestX, bestY, bestBounds
		}
	}

	// 低差异候选仍重叠时才扫描剩余像素位置，避免最宽字符组合存在空位却因抽样未命中而重叠。
	horizontalCount := maxX - minX + 1
	verticalCount := maxY - minY + 1
	horizontalStart := int(math.Floor(horizontalPhase * float64(horizontalCount)))
	verticalStart := int(math.Floor(verticalPhase * float64(verticalCount)))
	for verticalOffset := range verticalCount {
		y := minY + (verticalStart+verticalOffset)%verticalCount
		for horizontalOffset := range horizontalCount {
			x := minX + (horizontalStart+horizontalOffset)%horizontalCount
			inkBounds := image.Rect(x+glyphMinX, y+glyphMinY, x+glyphMaxX, y+glyphMaxY)
			overlap := loginCaptchaBackgroundOverlapArea(inkBounds, occupied)
			if overlap < bestOverlap {
				bestOverlap, bestX, bestY, bestBounds = overlap, x, y, inkBounds
			}
			if overlap == 0 {
				return bestX, bestY, bestBounds
			}
		}
	}
	return bestX, bestY, bestBounds
}

// loginCaptchaBinaryRadicalInverse 将连续候选序号映射为均匀覆盖 [0,1) 的二进制反序列，供横向候选搜索使用。
func loginCaptchaBinaryRadicalInverse(value int) float64 {
	position := 0.0
	weight := 0.5
	for value > 0 {
		position += float64(value&1) * weight
		value >>= 1
		weight *= 0.5
	}
	return position
}

// loginCaptchaBackgroundOverlapArea 计算候选字形与既有字形的总交叠面积；边界相邻不算重叠，不强制预留字符间距。
func loginCaptchaBackgroundOverlapArea(candidate image.Rectangle, occupied []image.Rectangle) int {
	total := 0
	for _, current := range occupied {
		intersection := candidate.Intersect(current)
		if !intersection.Empty() {
			total += intersection.Dx() * intersection.Dy()
		}
	}
	return total
}

// drawLoginCaptchaForegroundCurves 在文字上方绘制多色正弦曲线，破坏纯色字符连通区域但保留人工可辨识轮廓。
func drawLoginCaptchaForegroundCurves(canvas *image.NRGBA, random *captchaImageRandom) {
	for range loginCaptchaForegroundCurveCount {
		baseY := 12 + random.intn(loginCaptchaImageHeight-24)
		amplitude := 2 + random.intn(3)
		wavelength := 28 + random.intn(20)
		phase := float64(random.intn(360)) * math.Pi / 180
		colorOffset := random.intn(len(loginCaptchaForegroundCurveColors))
		previousY := baseY
		for x := 1; x < loginCaptchaImageWidth; x++ {
			angle := float64(x)*2*math.Pi/float64(wavelength) + phase
			currentY := baseY + int(math.Round(float64(amplitude)*math.Sin(angle)))
			curveColor := loginCaptchaForegroundCurveColors[(colorOffset+x/12)%len(loginCaptchaForegroundCurveColors)]
			drawLoginCaptchaLine(canvas, x-1, previousY, x, currentY, curveColor)
			previousY = currentY
		}
	}
}

// drawLoginCaptchaRainbowBands 绘制两条错开半周期的动态波浪带；frameCount 来自当前图片，避免运动周期固定为 8 帧。
func drawLoginCaptchaRainbowBands(canvas *image.NRGBA, frameIndex int, frameCount int) {
	for bandIndex := range loginCaptchaRainbowBandCount {
		progressIndex := (frameIndex + bandIndex*frameCount/loginCaptchaRainbowBandCount) % frameCount
		progress := float64(progressIndex) / float64(frameCount-1)
		left := -loginCaptchaRainbowBandWidth + int(math.Round(progress*float64(loginCaptchaImageWidth+loginCaptchaRainbowBandWidth)))
		alpha := uint8(loginCaptchaRainbowPrimaryAlpha)
		if bandIndex == 1 {
			alpha = loginCaptchaRainbowSecondaryAlpha
		}
		drawLoginCaptchaRainbowBand(canvas, left, frameIndex, bandIndex, alpha, frameCount)
	}
}

// drawLoginCaptchaRainbowBand 绘制单条 30% 宽的斜向波浪带，并按帧推进彩虹色段。
func drawLoginCaptchaRainbowBand(canvas *image.NRGBA, left int, frameIndex int, bandIndex int, alpha uint8, frameCount int) {
	slant := math.Tan(float64(loginCaptchaRainbowSlantDegrees) * math.Pi / 180)
	phase := float64(frameIndex) * 2 * math.Pi / float64(frameCount)
	for y := 0; y < loginCaptchaImageHeight; y++ {
		wave := int(math.Round(float64(loginCaptchaRainbowWaveAmplitude) * math.Sin(float64(y)*4*math.Pi/float64(loginCaptchaImageHeight)+phase)))
		slantOffset := int(math.Round(float64(y-loginCaptchaImageHeight/2) * slant))
		startX := left + wave + slantOffset
		bandColor := loginCaptchaRainbowColor(y, frameIndex, bandIndex)
		for x := startX; x < startX+loginCaptchaRainbowBandWidth; x++ {
			blendLoginCaptchaPixel(canvas, x, y, bandColor, alpha)
		}
		for lineIndex := range loginCaptchaRainbowSolidLineCount {
			lineX := startX + (lineIndex+1)*loginCaptchaRainbowBandWidth/(loginCaptchaRainbowSolidLineCount+1)
			lineColor := loginCaptchaRainbowColors[(frameIndex+bandIndex*3+lineIndex*3+y/12)%len(loginCaptchaRainbowColors)]
			if image.Pt(lineX, y).In(canvas.Bounds()) {
				canvas.Set(lineX, y, lineColor)
			}
		}
	}
}

// drawLoginCaptchaHorizontalRainbowLines 绘制 3 条不同相位的横向彩虹正弦线；运动周期跟随当前图片的随机帧数。
func drawLoginCaptchaHorizontalRainbowLines(canvas *image.NRGBA, frameIndex int, frameCount int) {
	for lineIndex := range loginCaptchaHorizontalRainbowLineCount {
		framePhase := float64(frameIndex)*2*math.Pi/float64(frameCount) + float64(lineIndex)*2*math.Pi/float64(loginCaptchaHorizontalRainbowLineCount)
		centerOffset := (lineIndex - loginCaptchaHorizontalRainbowLineCount/2) * loginCaptchaHorizontalRainbowLineSpacing
		centerY := loginCaptchaImageHeight/2 + centerOffset + int(math.Round(float64(loginCaptchaHorizontalSweepAmplitude)*math.Sin(framePhase)))
		previousY := centerY
		for x := 1; x < loginCaptchaImageWidth; x++ {
			wavePhase := float64(x)*4*math.Pi/float64(loginCaptchaImageWidth) + framePhase + float64(lineIndex)*math.Pi/3
			currentY := centerY + int(math.Round(float64(loginCaptchaHorizontalWaveAmplitude)*math.Sin(wavePhase)))
			lineColor := loginCaptchaRainbowColors[(frameIndex+lineIndex*3+x/10)%len(loginCaptchaRainbowColors)]
			drawLoginCaptchaLine(canvas, x-1, previousY, x, currentY, lineColor)
			previousY = currentY
		}
	}
}

// loginCaptchaRainbowColor 返回波浪带当前位置的渐变色；逐帧偏移四分之一色段，避免颜色静止。
func loginCaptchaRainbowColor(y int, frameIndex int, bandIndex int) color.RGBA {
	colorCount := len(loginCaptchaRainbowColors)
	scaled := y*colorCount*256/loginCaptchaImageHeight + frameIndex*64 + bandIndex*colorCount*128
	colorIndex := scaled / 256 % colorCount
	nextIndex := (colorIndex + 1) % colorCount
	fraction := scaled % 256
	return interpolateLoginCaptchaColor(loginCaptchaRainbowColors[colorIndex], loginCaptchaRainbowColors[nextIndex], fraction)
}

// interpolateLoginCaptchaColor 按 0-255 的比例插值相邻彩虹色，避免 GIF 波浪带出现大块色阶。
func interpolateLoginCaptchaColor(from color.RGBA, to color.RGBA, fraction int) color.RGBA {
	inverse := 255 - fraction
	return color.RGBA{
		R: uint8((int(from.R)*inverse + int(to.R)*fraction) / 255),
		G: uint8((int(from.G)*inverse + int(to.G)*fraction) / 255),
		B: uint8((int(from.B)*inverse + int(to.B)*fraction) / 255),
		A: 255,
	}
}

// blendLoginCaptchaPixel 以指定不透明度混合波浪带像素；越界部分自然裁剪在验证码画布之外。
func blendLoginCaptchaPixel(canvas *image.NRGBA, x int, y int, overlay color.RGBA, alpha uint8) {
	point := image.Pt(x, y)
	if !point.In(canvas.Bounds()) {
		return
	}
	base := canvas.NRGBAAt(x, y)
	overlayWeight := int(alpha)
	baseWeight := 255 - overlayWeight
	canvas.SetNRGBA(x, y, color.NRGBA{
		R: uint8((int(base.R)*baseWeight + int(overlay.R)*overlayWeight) / 255),
		G: uint8((int(base.G)*baseWeight + int(overlay.G)*overlayWeight) / 255),
		B: uint8((int(base.B)*baseWeight + int(overlay.B)*overlayWeight) / 255),
		A: 255,
	})
}

// drawLoginCaptchaText 按字符槽位绘制文本；每帧随机错位和偏转，hiddenMask 允许无序隐藏零至两个字符。
func drawLoginCaptchaText(canvas *image.NRGBA, code string, random *captchaImageRandom, frameIndex int, frameCount int, hiddenMask uint64) error {
	runes := []rune(code)
	if len(runes) == 0 {
		return errors.New("验证码内容不能为空")
	}
	context := freetype.NewContext()
	context.SetDPI(loginCaptchaDPI)
	context.SetFont(loginCaptchaFont)
	context.SetFontSize(loginCaptchaFontSize)
	context.SetHinting(font.HintingFull)

	face := truetype.NewFace(loginCaptchaFont, &truetype.Options{
		DPI:     loginCaptchaDPI,
		Hinting: font.HintingFull,
		Size:    loginCaptchaFontSize,
	})
	defer face.Close()

	metrics := face.Metrics()
	ascent := metrics.Ascent.Ceil()
	descent := metrics.Descent.Ceil()
	baseline := (loginCaptchaImageHeight-ascent-descent)/2 + ascent
	safeBounds := image.Rect(
		loginCaptchaCharacterPadding,
		loginCaptchaCharacterPadding,
		loginCaptchaImageWidth-loginCaptchaCharacterPadding,
		loginCaptchaImageHeight-loginCaptchaCharacterPadding,
	)
	drawer := font.Drawer{Face: face}

	for index, char := range runes {
		textColor := random.color(loginCaptchaTextColors)
		offsetX := random.offset(loginCaptchaCharacterOffsetX)
		offsetY := random.offset(loginCaptchaCharacterOffsetY)
		baseTilt := random.offset(loginCaptchaCharacterBaseTiltDegrees)
		jumpPhase := float64(frameIndex)*2*math.Pi/float64(frameCount) + float64(index)*math.Pi/2
		jumpY := int(math.Round(float64(loginCaptchaCharacterJumpAmplitude) * math.Sin(jumpPhase)))
		text := string(char)
		advance := drawer.MeasureString(text).Ceil()
		cellLeft := safeBounds.Min.X + index*safeBounds.Dx()/len(runes)
		cellRight := safeBounds.Min.X + (index+1)*safeBounds.Dx()/len(runes)
		cellWidth := cellRight - cellLeft
		x := clampInt(cellLeft+(cellWidth-advance)/2+offsetX, cellLeft, max(cellLeft, cellRight-advance))
		y := baseline + offsetY + jumpY
		if hiddenMask&(uint64(1)<<index) != 0 {
			continue
		}
		glyphLeft := cellLeft - loginCaptchaCharacterGlyphPadding
		glyphTop := -loginCaptchaCharacterGlyphPadding
		glyph := image.NewNRGBA(image.Rect(
			0,
			0,
			cellWidth+loginCaptchaCharacterGlyphPadding*2,
			loginCaptchaImageHeight+loginCaptchaCharacterGlyphPadding*2,
		))
		context.SetClip(glyph.Bounds())
		context.SetDst(glyph)
		context.SetSrc(image.NewUniform(textColor))
		if _, err := context.DrawString(text, freetype.Pt(x-glyphLeft, y-glyphTop)); err != nil {
			return errors.Wrap(err, "绘制登录验证码文字失败")
		}
		tilt := loginCaptchaCharacterTilt(baseTilt, frameIndex, index, frameCount)
		drawLoginCaptchaRotatedGlyph(canvas, glyph, glyphLeft, glyphTop, tilt, safeBounds)
	}
	return nil
}

// loginCaptchaCharacterTilt 计算主字符当前帧偏角；随机基础角叠加错相位摆动，并跟随本张图片的随机帧数完成周期。
func loginCaptchaCharacterTilt(baseTilt int, frameIndex int, characterIndex int, frameCount int) float64 {
	phase := float64(frameIndex)*2*math.Pi/float64(frameCount) + float64(characterIndex)*math.Pi/2
	return float64(baseTilt) + float64(loginCaptchaCharacterTiltSwingDegrees)*math.Sin(phase)
}

// drawLoginCaptchaRotatedGlyph 旋转字符后整体平移进 3px 安全区，保留完整笔画而不是依赖画布裁剪。
func drawLoginCaptchaRotatedGlyph(canvas *image.NRGBA, glyph *image.NRGBA, targetLeft int, targetTop int, angleDegrees float64, safeBounds image.Rectangle) {
	angle := angleDegrees * math.Pi / 180
	cosine := math.Cos(angle)
	sine := math.Sin(angle)
	bounds := glyph.Bounds()
	centerX := float64(bounds.Min.X+bounds.Max.X-1) / 2
	centerY := float64(bounds.Min.Y+bounds.Max.Y-1) / 2
	rotated := image.NewNRGBA(bounds)
	inkBounds := image.Rectangle{}
	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			deltaX := float64(x) - centerX
			deltaY := float64(y) - centerY
			sourceX := int(math.Round(cosine*deltaX + sine*deltaY + centerX))
			sourceY := int(math.Round(-sine*deltaX + cosine*deltaY + centerY))
			if !image.Pt(sourceX, sourceY).In(bounds) {
				continue
			}
			source := glyph.NRGBAAt(sourceX, sourceY)
			if source.A == 0 {
				continue
			}
			rotated.SetNRGBA(x, y, source)
			pointBounds := image.Rect(x, y, x+1, y+1)
			if inkBounds.Empty() {
				inkBounds = pointBounds
			} else {
				inkBounds = inkBounds.Union(pointBounds)
			}
		}
	}
	if inkBounds.Empty() {
		return
	}
	targetLeft = clampInt(targetLeft, safeBounds.Min.X-inkBounds.Min.X, safeBounds.Max.X-inkBounds.Max.X)
	targetTop = clampInt(targetTop, safeBounds.Min.Y-inkBounds.Min.Y, safeBounds.Max.Y-inkBounds.Max.Y)
	for y := inkBounds.Min.Y; y < inkBounds.Max.Y; y++ {
		for x := inkBounds.Min.X; x < inkBounds.Max.X; x++ {
			source := rotated.NRGBAAt(x, y)
			if source.A != 0 {
				blendLoginCaptchaGlyphPixel(canvas, targetLeft+x, targetTop+y, source)
			}
		}
	}
}

// blendLoginCaptchaGlyphPixel 把带抗锯齿 alpha 的字符像素合成到验证码底图；调用方已把完整字形移入安全区。
func blendLoginCaptchaGlyphPixel(canvas *image.NRGBA, x int, y int, source color.NRGBA) {
	if !image.Pt(x, y).In(canvas.Bounds()) {
		return
	}
	if source.A == 255 {
		canvas.SetNRGBA(x, y, source)
		return
	}
	destination := canvas.NRGBAAt(x, y)
	sourceWeight := int(source.A)
	destinationWeight := 255 - sourceWeight
	canvas.SetNRGBA(x, y, color.NRGBA{
		R: uint8((int(source.R)*sourceWeight + int(destination.R)*destinationWeight) / 255),
		G: uint8((int(source.G)*sourceWeight + int(destination.G)*destinationWeight) / 255),
		B: uint8((int(source.B)*sourceWeight + int(destination.B)*destinationWeight) / 255),
		A: 255,
	})
}

// drawLoginCaptchaSnowflake 绘制最底层浅色雪花。
func drawLoginCaptchaSnowflake(canvas *image.NRGBA, centerX int, centerY int, radius int, markColor color.RGBA) {
	drawLoginCaptchaLine(canvas, centerX-radius, centerY, centerX+radius, centerY, markColor)
	drawLoginCaptchaLine(canvas, centerX, centerY-radius, centerX, centerY+radius, markColor)
	drawLoginCaptchaLine(canvas, centerX-radius+1, centerY-radius+1, centerX+radius-1, centerY+radius-1, markColor)
	drawLoginCaptchaLine(canvas, centerX-radius+1, centerY+radius-1, centerX+radius-1, centerY-radius+1, markColor)
}

// drawLoginCaptchaStar 绘制最底层浅色五星。
func drawLoginCaptchaStar(canvas *image.NRGBA, centerX int, centerY int, radius int, markColor color.RGBA) {
	drawLoginCaptchaLine(canvas, centerX, centerY-radius, centerX+radius, centerY+radius-1, markColor)
	drawLoginCaptchaLine(canvas, centerX+radius, centerY+radius-1, centerX-radius, centerY-1, markColor)
	drawLoginCaptchaLine(canvas, centerX-radius, centerY-1, centerX+radius, centerY-1, markColor)
	drawLoginCaptchaLine(canvas, centerX+radius, centerY-1, centerX-radius, centerY+radius-1, markColor)
	drawLoginCaptchaLine(canvas, centerX-radius, centerY+radius-1, centerX, centerY-radius, markColor)
}

// drawLoginCaptchaLine 使用 Bresenham 算法绘制单像素线。
func drawLoginCaptchaLine(canvas *image.NRGBA, x0 int, y0 int, x1 int, y1 int, lineColor color.RGBA) {
	dx := absInt(x1 - x0)
	dy := -absInt(y1 - y0)
	stepX := -1
	if x0 < x1 {
		stepX = 1
	}
	stepY := -1
	if y0 < y1 {
		stepY = 1
	}
	err := dx + dy
	for {
		if image.Pt(x0, y0).In(canvas.Bounds()) {
			canvas.Set(x0, y0, lineColor)
		}
		if x0 == x1 && y0 == y1 {
			return
		}
		e2 := err * 2
		if e2 >= dy {
			err += dy
			x0 += stepX
		}
		if e2 <= dx {
			err += dx
			y0 += stepY
		}
	}
}

// clampInt 把整数限制到指定闭区间。
func clampInt(value int, minValue int, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

// absInt 返回整数绝对值。
func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}
