// Package ipregion 提供本地 XDB IP 归属地查询。
package ipregion

import (
	"bytes"
	"encoding/binary"
	"io"
	"net/netip"
	"os"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"admin/internal/config"

	"github.com/Is999/go-utils/errors"
	"github.com/lionsoul2014/ip2region/binding/golang/xdb"
)

const (
	// regionLocal 表示私有网络地址，不进入公共 IP 数据库查询。
	regionLocal = "内网"
	// regionLoopback 表示本机回环地址。
	regionLoopback = "本机"
	// maxXDBTotalBytes 限制 IPv4 与 IPv6 XDB 的启动期聚合内存占用。
	maxXDBTotalBytes int64 = 128 << 20
	// maxXDBRegionBytes 对齐 XDB uint16 数据长度，允许在内置五字段后追加自定义字段。
	maxXDBRegionBytes = 1<<16 - 1
	// maxDisplayRegionRunes 保守限制后台展示值长度，正常国家、省、市远小于该上限。
	maxDisplayRegionRunes = 100
	// xdbVectorEnd 是固定头部与完整向量索引之后的首个数据字节偏移。
	xdbVectorEnd = xdb.HeaderInfoLength + xdb.VectorIndexRows*xdb.VectorIndexCols*xdb.VectorIndexSize
)

// carrierGradeNATPrefix 是运营商级 NAT 共享地址段；显式构造地址，避免 MustParsePrefix 在包初始化阶段触发不可恢复 panic。
var carrierGradeNATPrefix = netip.PrefixFrom(netip.AddrFrom4([4]byte{100, 64, 0, 0}), 10)

// Locator 封装 ip2region 的并发安全查询服务。
type Locator struct {
	mu      sync.RWMutex // 保护查询数据库关闭与并发查询的生命周期边界
	enabled bool         // 是否启用本地 IP 归属地解析
	ipv4    *memoryDB    // IPv4 只读内存数据库，未配置时为空
	ipv6    *memoryDB    // IPv6 只读内存数据库，未配置时为空
}

// memoryDB 保存一份只读 XDB 内容，并为每次并发查询复用独立 Searcher 状态。
type memoryDB struct {
	version   *xdb.Version // XDB IP 版本
	content   []byte       // 启动期完整读入的只读 XDB 内容
	searchers sync.Pool    // Searcher 自身非并发安全，按调用独占复用
}

// New 根据启动期配置加载并校验 XDB 数据库。
func New(cfg config.IPRegionConfig) (*Locator, error) {
	locator := &Locator{}
	if !cfg.Enabled {
		return locator, nil
	}

	v4Path := strings.TrimSpace(cfg.IPv4XDBPath)
	v6Path := strings.TrimSpace(cfg.IPv6XDBPath)
	if v4Path == "" && v6Path == "" {
		return nil, errors.Errorf("ip_region 已启用但未配置 ipv4_xdb_path 或 ipv6_xdb_path")
	}
	remainingBytes := maxXDBTotalBytes
	if v4Path != "" {
		var err error
		locator.ipv4, err = loadMemoryDB(v4Path, xdb.IPv4, remainingBytes)
		if err != nil {
			return nil, errors.Wrap(err, "加载 IPv4 XDB 文件失败")
		}
		remainingBytes -= int64(len(locator.ipv4.content))
	}
	if v6Path != "" {
		var err error
		locator.ipv6, err = loadMemoryDB(v6Path, xdb.IPv6, remainingBytes)
		if err != nil {
			return nil, errors.Wrap(err, "加载 IPv6 XDB 文件失败")
		}
	}
	locator.enabled = true
	return locator, nil
}

// loadMemoryDB 在剩余聚合额度内精确读取并完整校验 XDB，返回后不保留文件句柄。
func loadMemoryDB(path string, expected *xdb.Version, remainingBytes int64) (*memoryDB, error) {
	if expected == nil {
		return nil, errors.Errorf("XDB 期望 IP 版本不能为空")
	}
	if remainingBytes <= 0 {
		return nil, errors.Errorf("XDB 聚合大小超过 %d 字节", maxXDBTotalBytes)
	}
	handle, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "打开 XDB 文件失败")
	}
	defer func() { _ = handle.Close() }()

	fileInfo, err := handle.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "读取 XDB 文件信息失败")
	}
	if !fileInfo.Mode().IsRegular() {
		return nil, errors.Errorf("XDB 路径必须是普通文件: %s", path)
	}
	if fileInfo.Size() <= 0 {
		return nil, errors.Errorf("XDB 文件不能为空: path=%s", path)
	}
	if fileInfo.Size() > remainingBytes {
		return nil, errors.Errorf("XDB 聚合大小超过 %d 字节: path=%s size=%d remaining=%d", maxXDBTotalBytes, path, fileInfo.Size(), remainingBytes)
	}

	content := make([]byte, int(fileInfo.Size()))
	if _, err = io.ReadFull(handle, content); err != nil {
		return nil, errors.Wrap(err, "读取 XDB 文件内容失败")
	}
	var extra [1]byte
	if size, readErr := handle.Read(extra[:]); size != 0 || readErr != io.EOF {
		if readErr != nil {
			return nil, errors.Wrap(readErr, "确认 XDB 文件结尾失败")
		}
		return nil, errors.Errorf("XDB 文件在加载期间增长: path=%s", path)
	}
	loadedInfo, err := handle.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "再次读取 XDB 文件信息失败")
	}
	pathInfo, err := os.Stat(path)
	if err != nil {
		return nil, errors.Wrap(err, "再次确认 XDB 文件路径失败")
	}
	if !sameXDBFile(fileInfo, loadedInfo) || !sameXDBFile(fileInfo, pathInfo) {
		return nil, errors.Errorf("XDB 文件在加载期间发生变化: path=%s", path)
	}
	if err = validateXDB(content, expected); err != nil {
		return nil, errors.Wrap(err, "校验 XDB 文件内容失败")
	}
	return &memoryDB{version: expected, content: content}, nil
}

// sameXDBFile 比较文件身份及加载期间可观察的大小、模式和修改时间。
func sameXDBFile(expected, actual os.FileInfo) bool {
	return os.SameFile(expected, actual) &&
		expected.Size() == actual.Size() &&
		expected.Mode() == actual.Mode() &&
		expected.ModTime().Equal(actual.ModTime())
}

// validateXDB 校验 XDB 头部、向量、段索引和数据区的完整结构。
func validateXDB(content []byte, expected *xdb.Version) error {
	if expected == nil || expected.Bytes <= 0 || expected.SegmentIndexSize != expected.Bytes*2+6 {
		return errors.Errorf("XDB 期望 IP 版本无效")
	}
	if len(content) < xdbVectorEnd+expected.SegmentIndexSize {
		return errors.Errorf("XDB 文件长度不足: actual=%d minimum=%d", len(content), xdbVectorEnd+expected.SegmentIndexSize)
	}
	header, err := xdb.NewHeader(content[:xdb.HeaderInfoLength])
	if err != nil {
		return errors.Wrap(err, "解析 XDB 文件头失败")
	}
	if header.Version != xdb.Structure30 {
		return errors.Errorf("XDB 结构版本不受支持: actual=%d expected=%d", header.Version, xdb.Structure30)
	}
	if header.IndexPolicy != xdb.VectorIndexPolicy {
		return errors.Errorf("XDB 索引策略不受支持: actual=%s expected=%s", header.IndexPolicy.String(), xdb.VectorIndexPolicy.String())
	}
	actual, err := xdb.VersionFromHeader(header)
	if err != nil {
		return errors.Wrap(err, "识别 XDB IP 版本失败")
	}
	if actual.Id != expected.Id {
		return errors.Errorf("XDB IP 版本不匹配: actual=%s expected=%s", actual.Name, expected.Name)
	}
	if header.RuntimePtrBytes != 4 {
		return errors.Errorf("XDB 指针宽度不受支持: actual=%d expected=4", header.RuntimePtrBytes)
	}

	indexStart := uint64(header.StartIndexPtr)
	indexEnd := uint64(header.EndIndexPtr)
	segmentSize := uint64(expected.SegmentIndexSize)
	contentSize := uint64(len(content))
	if indexStart <= xdbVectorEnd {
		return errors.Errorf("XDB 段索引起始指针无效: actual=%d minimum=%d", indexStart, xdbVectorEnd+1)
	}
	if indexEnd < indexStart || (indexEnd-indexStart)%segmentSize != 0 {
		return errors.Errorf("XDB 段索引边界无效: start=%d end=%d segment_size=%d", indexStart, indexEnd, segmentSize)
	}
	indexLimit := indexEnd + segmentSize
	if indexLimit != contentSize {
		return errors.Errorf("XDB 段索引未精确覆盖文件结尾: index_limit=%d file_size=%d", indexLimit, contentSize)
	}
	if err = validateXDBVector(content, indexStart, indexEnd, indexLimit, segmentSize); err != nil {
		return err
	}
	if err = validateXDBSegments(content, expected, indexStart, indexEnd, segmentSize); err != nil {
		return err
	}
	return validateXDBVectorRanges(content, expected, indexStart, indexEnd, segmentSize)
}

// validateXDBVector 校验全部向量指针的成对、边界、对齐和顺序约束。
func validateXDBVector(content []byte, indexStart, indexEnd, indexLimit, segmentSize uint64) error {
	var previousStart, previousEnd uint64
	found := false
	for offset := xdb.HeaderInfoLength; offset < xdbVectorEnd; offset += xdb.VectorIndexSize {
		start := uint64(binary.LittleEndian.Uint32(content[offset:]))
		end := uint64(binary.LittleEndian.Uint32(content[offset+4:]))
		if start == 0 && end == 0 {
			continue
		}
		if start == 0 || end == 0 {
			return errors.Errorf("XDB 向量指针必须成对存在: offset=%d start=%d end=%d", offset, start, end)
		}
		if start < indexStart || start > indexEnd || end < start || end > indexLimit {
			return errors.Errorf("XDB 向量指针越界: offset=%d start=%d end=%d", offset, start, end)
		}
		if (start-indexStart)%segmentSize != 0 || (end-indexStart)%segmentSize != 0 {
			return errors.Errorf("XDB 向量指针未按段索引对齐: offset=%d start=%d end=%d", offset, start, end)
		}
		if found && (start < previousStart || end < previousEnd) {
			return errors.Errorf("XDB 向量指针顺序错误: offset=%d start=%d end=%d", offset, start, end)
		}
		if !found && start != indexStart {
			return errors.Errorf("XDB 首个向量未指向首段索引: actual=%d expected=%d", start, indexStart)
		}
		found = true
		previousStart = start
		previousEnd = end
	}
	if !found {
		return errors.Errorf("XDB 向量索引为空")
	}
	if previousEnd != indexLimit {
		return errors.Errorf("XDB 末个向量未覆盖段索引结尾: actual=%d expected=%d", previousEnd, indexLimit)
	}
	return nil
}

// validateXDBSegments 校验段 IP 范围、排序及数据指针和 UTF-8 内容。
func validateXDBSegments(content []byte, version *xdb.Version, indexStart, indexEnd, segmentSize uint64) error {
	var previousEnd []byte
	for offset := indexStart; offset <= indexEnd; offset += segmentSize {
		record := content[int(offset):int(offset+segmentSize)]
		startIP := record[:version.Bytes]
		endIP := record[version.Bytes : version.Bytes*2]
		if compareXDBIP(version, startIP, endIP) > 0 {
			return errors.Errorf("XDB 段 IP 范围倒置: offset=%d", offset)
		}
		if previousEnd != nil && compareXDBIP(version, previousEnd, startIP) >= 0 {
			return errors.Errorf("XDB 段 IP 范围重叠或乱序: offset=%d", offset)
		}
		previousEnd = endIP

		dataOffset := version.Bytes * 2
		dataLength := uint64(binary.LittleEndian.Uint16(record[dataOffset:]))
		dataPointer := uint64(binary.LittleEndian.Uint32(record[dataOffset+2:]))
		dataEnd := dataPointer + dataLength
		if dataLength == 0 || dataLength > maxXDBRegionBytes || dataPointer < xdbVectorEnd || dataEnd > indexStart {
			return errors.Errorf("XDB 段数据指针无效: offset=%d data_ptr=%d data_len=%d", offset, dataPointer, dataLength)
		}
		region := content[int(dataPointer):int(dataEnd)]
		if !utf8.Valid(region) {
			return errors.Errorf("XDB 段数据不是有效 UTF-8: offset=%d data_ptr=%d", offset, dataPointer)
		}
		if bytes.Count(region, []byte{'|'}) < 4 || bytes.IndexFunc(region, unicode.IsControl) >= 0 {
			return errors.Errorf("XDB 段数据格式无效: offset=%d data_ptr=%d", offset, dataPointer)
		}
	}
	return nil
}

// validateXDBVectorRanges 校验每个双字节前缀精确指向覆盖该范围的段索引区间。
func validateXDBVectorRanges(content []byte, version *xdb.Version, indexStart, indexEnd, segmentSize uint64) error {
	candidate := indexStart
	for prefix := 0; prefix < xdb.VectorIndexRows*xdb.VectorIndexCols; prefix++ {
		lower, upper := xdbPrefixRange(version, uint16(prefix))
		for candidate <= indexEnd {
			record := content[int(candidate):int(candidate+segmentSize)]
			if compareXDBIP(version, record[version.Bytes:version.Bytes*2], lower[:version.Bytes]) >= 0 {
				break
			}
			candidate += segmentSize
		}

		var expectedStart, expectedEnd uint64
		if candidate <= indexEnd {
			record := content[int(candidate):int(candidate+segmentSize)]
			if compareXDBIP(version, record[:version.Bytes], upper[:version.Bytes]) <= 0 {
				expectedStart = candidate
				expectedEnd = candidate
				for expectedEnd <= indexEnd {
					record = content[int(expectedEnd):int(expectedEnd+segmentSize)]
					if compareXDBIP(version, record[:version.Bytes], upper[:version.Bytes]) > 0 {
						break
					}
					expectedEnd += segmentSize
				}
			}
		}

		offset := xdb.HeaderInfoLength + prefix*xdb.VectorIndexSize
		actualStart := uint64(binary.LittleEndian.Uint32(content[offset:]))
		actualEnd := uint64(binary.LittleEndian.Uint32(content[offset+4:]))
		if actualStart != expectedStart || actualEnd != expectedEnd {
			return errors.Errorf("XDB 向量范围与段索引不一致: prefix=%04x actual=%d-%d expected=%d-%d", prefix, actualStart, actualEnd, expectedStart, expectedEnd)
		}
	}
	return nil
}

// xdbPrefixRange 返回 XDB 字节序下指定双字节网络前缀的最小与最大 IP。
func xdbPrefixRange(version *xdb.Version, prefix uint16) (lower, upper [16]byte) {
	if version.Id == xdb.IPv4VersionNo {
		binary.LittleEndian.PutUint32(lower[:4], uint32(prefix)<<16)
		binary.LittleEndian.PutUint32(upper[:4], uint32(prefix)<<16|0xffff)
		return lower, upper
	}
	lower[0] = byte(prefix >> 8)
	lower[1] = byte(prefix)
	copy(upper[:], lower[:])
	for index := 2; index < version.Bytes; index++ {
		upper[index] = 0xff
	}
	return lower, upper
}

// compareXDBIP 按 XDB 段索引中的字节序比较同版本 IP。
func compareXDBIP(version *xdb.Version, left, right []byte) int {
	if version.Id == xdb.IPv4VersionNo {
		leftValue := binary.LittleEndian.Uint32(left)
		rightValue := binary.LittleEndian.Uint32(right)
		switch {
		case leftValue < rightValue:
			return -1
		case leftValue > rightValue:
			return 1
		default:
			return 0
		}
	}
	return bytes.Compare(left, right)
}

// Close 释放 XDB 内存引用；进行中的只读查询会使用自己的快照安全完成。
func (l *Locator) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.enabled = false
	l.ipv4 = nil
	l.ipv6 = nil
}

// Enabled 返回当前实例是否已加载并启用归属地库。
func (l *Locator) Enabled() bool {
	if l == nil {
		return false
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.enabled
}

// Lookup 返回适于后台展示的国家、省、市归属地；未命中或异常时返回空字符串。
func (l *Locator) Lookup(ip string) string {
	if l == nil {
		return ""
	}
	l.mu.RLock()
	if !l.enabled {
		l.mu.RUnlock()
		return ""
	}
	ipv4 := l.ipv4
	ipv6 := l.ipv6
	l.mu.RUnlock()
	addr, err := netip.ParseAddr(strings.TrimSpace(ip))
	if err != nil {
		return ""
	}
	addr = addr.Unmap()
	if addr.Zone() != "" {
		addr = addr.WithZone("")
	}
	if addr.IsLoopback() {
		return regionLoopback
	}
	if addr.IsPrivate() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() || carrierGradeNATPrefix.Contains(addr) {
		return regionLocal
	}
	if !addr.IsGlobalUnicast() {
		return ""
	}
	database := ipv6
	if addr.Is4() {
		database = ipv4
	}
	if database == nil {
		return ""
	}
	region, err := database.search(addr.String())
	if err != nil {
		return ""
	}
	return displayRegion(region)
}

// search 独占一个 Searcher 执行纯内存查询，并把异常 XDB 导致的 panic 收敛为普通错误。
func (d *memoryDB) search(ip string) (region string, err error) {
	if d == nil || d.version == nil || len(d.content) == 0 {
		return "", errors.Errorf("XDB 内存数据库未初始化")
	}
	var searcher *xdb.Searcher
	if pooled := d.searchers.Get(); pooled != nil {
		searcher, _ = pooled.(*xdb.Searcher)
	}
	if searcher == nil {
		searcher, err = xdb.NewWithBuffer(d.version, d.content)
		if err != nil {
			return "", errors.Wrap(err, "创建 XDB 内存查询器失败")
		}
	}
	defer d.searchers.Put(searcher)
	defer func() {
		if recovered := recover(); recovered != nil {
			region = ""
			err = errors.Errorf("XDB 内存查询异常: %v", recovered)
		}
	}()
	return searcher.Search(ip)
}

// displayRegion 去除 ISP、ISO 编码与空占位，只保留国家、省、市。
func displayRegion(region string) string {
	if len(region) == 0 || len(region) > maxXDBRegionBytes || !utf8.ValidString(region) || strings.Count(region, "|") < 4 || strings.IndexFunc(region, unicode.IsControl) >= 0 {
		return ""
	}
	parts := strings.SplitN(region, "|", 4)[:3]
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" && part != "0" {
			values = append(values, part)
		}
	}
	result := strings.Join(values, " ")
	if utf8.RuneCountInString(result) > maxDisplayRegionRunes {
		return ""
	}
	return result
}
