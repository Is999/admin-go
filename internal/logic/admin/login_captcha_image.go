package admin

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"image"
	"image/color"
	"image/color/palette"
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
	// loginCaptchaPaddingX 表示验证码左右安全留白，避免字符贴边裁切。
	loginCaptchaPaddingX = 6
	// loginCaptchaGuideLineCount 表示每张验证码绘制 5 条背景淡线；线条先于文字绘制，避免遮挡字符。
	loginCaptchaGuideLineCount = 5
	// loginCaptchaSnowflakeCount 表示每张验证码固定绘制 4 个雪花，避免随机图案分配导致雪花缺失。
	loginCaptchaSnowflakeCount = 4
	// loginCaptchaNoiseMarkCount 表示每张验证码的图案噪声总数；4 个前景雪花之外的 5 个背景图案随机使用五星或圆点。
	loginCaptchaNoiseMarkCount = 9
	// loginCaptchaForegroundCurveCount 表示文字上方绘制 2 条彩色波浪曲线，用单像素交叉轨迹增加字符分割难度。
	loginCaptchaForegroundCurveCount = 2
	// loginCaptchaAnimationFrameCount 表示动态验证码包含 12 帧；配合单帧延迟后完整循环约 2.16 秒。
	loginCaptchaAnimationFrameCount = 12
	// loginCaptchaFrameDelayCentiseconds 表示 GIF 单帧停留 18 厘秒，即 180 毫秒。
	loginCaptchaFrameDelayCentiseconds = 18
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
	// loginCaptchaHorizontalSweepAmplitude 表示横向彩虹线逐帧上下摆动的最大距离，单位像素。
	loginCaptchaHorizontalSweepAmplitude = 8
	// loginCaptchaCharacterJumpAmplitude 表示每个字符逐帧上下跳动的最大距离，单位像素。
	loginCaptchaCharacterJumpAmplitude = 3
	// loginCaptchaRainbowSlantDegrees 表示波浪带相对垂直方向向右倾斜的角度。
	loginCaptchaRainbowSlantDegrees = 14
	// loginCaptchaRainbowPrimaryAlpha 表示主波浪带 40% 的不透明度。
	loginCaptchaRainbowPrimaryAlpha = 102
	// loginCaptchaRainbowSecondaryAlpha 表示次波浪带约 32% 的不透明度。
	loginCaptchaRainbowSecondaryAlpha = 82
	// loginCaptchaDPI 表示验证码字体渲染 DPI。
	loginCaptchaDPI = 72
	// loginCaptchaMimeType 表示验证码图片 MIME 类型。
	loginCaptchaMimeType = "image/gif"
	// loginCaptchaRandomBytes 表示单张图片预读的随机字节数；96 字节覆盖背景、线条、图案和文字偏移的当前取值次数。
	loginCaptchaRandomBytes = 96
)

// captchaImageRandom 保存单张图片的随机数据，避免每个噪声点单独读取系统随机源。
type captchaImageRandom struct {
	values [loginCaptchaRandomBytes]byte // values 保存本张图片使用的随机字节
	index  int                           // index 表示下一个待读取位置
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

// loginCaptchaNoiseMarkColors 定义登录验证码图案噪声颜色池。
var loginCaptchaNoiseMarkColors = []color.RGBA{
	{R: 198, G: 211, B: 234, A: 255},
	{R: 214, G: 198, B: 238, A: 255},
	{R: 232, G: 202, B: 221, A: 255},
	{R: 197, G: 226, B: 209, A: 255},
	{R: 236, G: 219, B: 162, A: 255},
	{R: 203, G: 226, B: 230, A: 255},
	{R: 230, G: 207, B: 186, A: 255},
	{R: 210, G: 222, B: 187, A: 255},
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

// drawLoginCaptchaGIF 编码循环 GIF；字符错相位跳动并从左到右逐个隐藏，两条彩虹波浪带按半周期错位移动。
func drawLoginCaptchaGIF(code string) ([]byte, error) {
	code = strings.TrimSpace(code)
	staticFrame, textRandom, err := drawLoginCaptchaStaticFrame(code)
	if err != nil {
		return nil, errors.Tag(err)
	}
	animation := gif.GIF{
		Image:     make([]*image.Paletted, 0, loginCaptchaAnimationFrameCount),
		Delay:     make([]int, 0, loginCaptchaAnimationFrameCount),
		Disposal:  make([]byte, 0, loginCaptchaAnimationFrameCount),
		LoopCount: 0,
	}
	for frameIndex := range loginCaptchaAnimationFrameCount {
		frame := image.NewNRGBA(staticFrame.Bounds())
		draw.Draw(frame, frame.Bounds(), staticFrame, staticFrame.Bounds().Min, draw.Src)
		frameRandom := textRandom
		hiddenCharacterIndex := loginCaptchaHiddenCharacterIndex(frameIndex, len([]rune(code)))
		if err = drawLoginCaptchaText(frame, code, &frameRandom, frameIndex, hiddenCharacterIndex); err != nil {
			return nil, errors.Tag(err)
		}
		drawLoginCaptchaForegroundSnowflakes(frame, &frameRandom)
		drawLoginCaptchaForegroundCurves(frame, &frameRandom)
		drawLoginCaptchaRainbowBands(frame, frameIndex)
		drawLoginCaptchaHorizontalRainbowLine(frame, frameIndex)
		paletted := image.NewPaletted(frame.Bounds(), palette.Plan9)
		draw.FloydSteinberg.Draw(paletted, paletted.Bounds(), frame, frame.Bounds().Min)
		animation.Image = append(animation.Image, paletted)
		animation.Delay = append(animation.Delay, loginCaptchaFrameDelayCentiseconds)
		animation.Disposal = append(animation.Disposal, gif.DisposalNone)
	}
	buffer := bytes.Buffer{}
	if err = gif.EncodeAll(&buffer, &animation); err != nil {
		return nil, errors.Wrap(err, "编码登录验证码 GIF 失败")
	}
	return buffer.Bytes(), nil
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
	background := random.color(loginCaptchaBackgroundColors)
	canvas := image.NewNRGBA(image.Rect(0, 0, loginCaptchaImageWidth, loginCaptchaImageHeight))
	draw.Draw(canvas, canvas.Bounds(), &image.Uniform{C: background}, image.Point{}, draw.Src)
	drawLoginCaptchaGuideLines(canvas, random)
	drawLoginCaptchaNoiseMarks(canvas, random)
	return canvas, *random, nil
}

// drawLoginCaptchaBaseFrame 绘制包含全部字符的静态检查帧，供边界测试确认文字不会被图片裁切。
func drawLoginCaptchaBaseFrame(code string) (*image.NRGBA, error) {
	canvas, textRandom, err := drawLoginCaptchaStaticFrame(code)
	if err != nil {
		return nil, errors.Tag(err)
	}
	if err = drawLoginCaptchaText(canvas, strings.TrimSpace(code), &textRandom, 0, -1); err != nil {
		return nil, errors.Tag(err)
	}
	drawLoginCaptchaForegroundSnowflakes(canvas, &textRandom)
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

// drawLoginCaptchaGuideLines 绘制淡背景线，提供轻量干扰但不覆盖最终文字。
func drawLoginCaptchaGuideLines(canvas *image.NRGBA, random *captchaImageRandom) {
	for range loginCaptchaGuideLineCount {
		lineColor := random.color(loginCaptchaGuideLineColors)
		startOffset := random.intn(loginCaptchaImageHeight - 16)
		endOffset := random.intn(loginCaptchaImageHeight - 16)
		drawLoginCaptchaLine(canvas, 4, 8+startOffset, loginCaptchaImageWidth-5, 8+endOffset, lineColor)
	}
}

// drawLoginCaptchaNoiseMarks 在文字下方绘制五星和圆点；雪花由前景函数按字符槽位单独覆盖。
func drawLoginCaptchaNoiseMarks(canvas *image.NRGBA, random *captchaImageRandom) {
	for range loginCaptchaNoiseMarkCount - loginCaptchaSnowflakeCount {
		markColor := random.color(loginCaptchaNoiseMarkColors)
		centerX := loginCaptchaPaddingX + random.intn(loginCaptchaImageWidth-loginCaptchaPaddingX*2)
		centerY := 6 + random.intn(loginCaptchaImageHeight-12)
		radius := random.intn(3)
		if random.intn(2) == 0 {
			drawLoginCaptchaStar(canvas, centerX, centerY, radius+3, markColor)
		} else {
			drawLoginCaptchaDot(canvas, centerX, centerY, radius+1, markColor)
		}
	}
}

// drawLoginCaptchaForegroundSnowflakes 按字符槽位覆盖雪花，使每个字符区域都有前景干扰且不会集中成块。
func drawLoginCaptchaForegroundSnowflakes(canvas *image.NRGBA, random *captchaImageRandom) {
	cellWidth := (loginCaptchaImageWidth - loginCaptchaPaddingX*2) / loginCaptchaSnowflakeCount
	for index := range loginCaptchaSnowflakeCount {
		markColor := random.color(loginCaptchaForegroundCurveColors)
		centerX := loginCaptchaPaddingX + index*cellWidth + cellWidth/2 + random.offset(cellWidth/4)
		centerY := loginCaptchaImageHeight/2 + random.offset(7)
		radius := 3 + random.intn(3)
		drawLoginCaptchaSnowflake(canvas, centerX, centerY, radius, markColor)
	}
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

// drawLoginCaptchaRainbowBands 绘制两条错开半周期的动态波浪带；波浪进入 GIF 像素，客户端无法通过关闭 CSS 移除。
func drawLoginCaptchaRainbowBands(canvas *image.NRGBA, frameIndex int) {
	for bandIndex := range loginCaptchaRainbowBandCount {
		progressIndex := (frameIndex + bandIndex*loginCaptchaAnimationFrameCount/loginCaptchaRainbowBandCount) % loginCaptchaAnimationFrameCount
		progress := float64(progressIndex) / float64(loginCaptchaAnimationFrameCount-1)
		left := -loginCaptchaRainbowBandWidth + int(math.Round(progress*float64(loginCaptchaImageWidth+loginCaptchaRainbowBandWidth)))
		alpha := uint8(loginCaptchaRainbowPrimaryAlpha)
		if bandIndex == 1 {
			alpha = loginCaptchaRainbowSecondaryAlpha
		}
		drawLoginCaptchaRainbowBand(canvas, left, frameIndex, bandIndex, alpha)
	}
}

// drawLoginCaptchaRainbowBand 绘制单条 30% 宽的斜向波浪带，并按帧推进彩虹色段。
func drawLoginCaptchaRainbowBand(canvas *image.NRGBA, left int, frameIndex int, bandIndex int, alpha uint8) {
	slant := math.Tan(float64(loginCaptchaRainbowSlantDegrees) * math.Pi / 180)
	phase := float64(frameIndex) * 2 * math.Pi / float64(loginCaptchaAnimationFrameCount)
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

// drawLoginCaptchaHorizontalRainbowLine 绘制横跨图片的单像素彩虹正弦线；线条逐帧上下摆动并覆盖经过的字符像素。
func drawLoginCaptchaHorizontalRainbowLine(canvas *image.NRGBA, frameIndex int) {
	framePhase := float64(frameIndex) * 2 * math.Pi / float64(loginCaptchaAnimationFrameCount)
	centerY := loginCaptchaImageHeight/2 + int(math.Round(float64(loginCaptchaHorizontalSweepAmplitude)*math.Sin(framePhase)))
	previousY := centerY
	for x := 1; x < loginCaptchaImageWidth; x++ {
		wavePhase := float64(x)*4*math.Pi/float64(loginCaptchaImageWidth) + framePhase
		currentY := centerY + int(math.Round(float64(loginCaptchaHorizontalWaveAmplitude)*math.Sin(wavePhase)))
		lineColor := loginCaptchaRainbowColors[(frameIndex+x/10)%len(loginCaptchaRainbowColors)]
		drawLoginCaptchaLine(canvas, x-1, previousY, x, currentY, lineColor)
		previousY = currentY
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

// drawLoginCaptchaSnowflake 绘制轻量雪花噪声。
func drawLoginCaptchaSnowflake(canvas *image.NRGBA, centerX int, centerY int, radius int, markColor color.RGBA) {
	drawLoginCaptchaLine(canvas, centerX-radius, centerY, centerX+radius, centerY, markColor)
	drawLoginCaptchaLine(canvas, centerX, centerY-radius, centerX, centerY+radius, markColor)
	drawLoginCaptchaLine(canvas, centerX-radius+1, centerY-radius+1, centerX+radius-1, centerY+radius-1, markColor)
	drawLoginCaptchaLine(canvas, centerX-radius+1, centerY+radius-1, centerX+radius-1, centerY-radius+1, markColor)
}

// drawLoginCaptchaStar 绘制轻量五星噪声。
func drawLoginCaptchaStar(canvas *image.NRGBA, centerX int, centerY int, radius int, markColor color.RGBA) {
	drawLoginCaptchaLine(canvas, centerX, centerY-radius, centerX+radius, centerY+radius-1, markColor)
	drawLoginCaptchaLine(canvas, centerX+radius, centerY+radius-1, centerX-radius, centerY-1, markColor)
	drawLoginCaptchaLine(canvas, centerX-radius, centerY-1, centerX+radius, centerY-1, markColor)
	drawLoginCaptchaLine(canvas, centerX+radius, centerY-1, centerX-radius, centerY+radius-1, markColor)
	drawLoginCaptchaLine(canvas, centerX-radius, centerY+radius-1, centerX, centerY-radius, markColor)
}

// drawLoginCaptchaDot 绘制小圆点噪声。
func drawLoginCaptchaDot(canvas *image.NRGBA, centerX int, centerY int, radius int, markColor color.RGBA) {
	for y := centerY - radius; y <= centerY+radius; y++ {
		for x := centerX - radius; x <= centerX+radius; x++ {
			if (x-centerX)*(x-centerX)+(y-centerY)*(y-centerY) <= radius*radius && image.Pt(x, y).In(canvas.Bounds()) {
				canvas.Set(x, y, markColor)
			}
		}
	}
}

// loginCaptchaHiddenCharacterIndex 返回当前帧隐藏的字符位置；12 帧按每 3 帧一组从左到右覆盖 4 个字符。
func loginCaptchaHiddenCharacterIndex(frameIndex int, characterCount int) int {
	if characterCount <= 0 {
		return -1
	}
	return frameIndex * characterCount / loginCaptchaAnimationFrameCount % characterCount
}

// drawLoginCaptchaText 按字符槽位绘制文本；各字符错相位跳动，hiddenCharacterIndex 所指字符保留空白。
func drawLoginCaptchaText(canvas *image.NRGBA, code string, random *captchaImageRandom, frameIndex int, hiddenCharacterIndex int) error {
	runes := []rune(code)
	if len(runes) == 0 {
		return errors.New("验证码内容不能为空")
	}
	context := freetype.NewContext()
	context.SetDPI(loginCaptchaDPI)
	context.SetClip(canvas.Bounds())
	context.SetDst(canvas)
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
	cellWidth := (loginCaptchaImageWidth - loginCaptchaPaddingX*2) / len(runes)
	drawer := font.Drawer{Face: face}

	for index, char := range runes {
		textColor := random.color(loginCaptchaTextColors)
		offsetX := random.offset(2)
		offsetY := random.offset(1)
		jumpPhase := float64(frameIndex)*2*math.Pi/float64(loginCaptchaAnimationFrameCount) + float64(index)*math.Pi/2
		jumpY := int(math.Round(float64(loginCaptchaCharacterJumpAmplitude) * math.Sin(jumpPhase)))
		text := string(char)
		advance := drawer.MeasureString(text).Ceil()
		x := loginCaptchaPaddingX + index*cellWidth + (cellWidth-advance)/2 + offsetX
		x = clampInt(x, loginCaptchaPaddingX/2, loginCaptchaImageWidth-loginCaptchaPaddingX-advance)
		y := clampInt(baseline+offsetY+jumpY, ascent+1, loginCaptchaImageHeight-descent-2)
		if index == hiddenCharacterIndex {
			continue
		}
		context.SetSrc(image.NewUniform(textColor))
		if _, err := context.DrawString(text, freetype.Pt(x, y)); err != nil {
			return errors.Wrap(err, "绘制登录验证码文字失败")
		}
	}
	return nil
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
