package configload

import (
	"net/netip"
	"net/url"
	"strings"

	"admin/internal/config"

	"github.com/Is999/go-utils/errors"
	"github.com/zeromicro/go-zero/rest"
)

const minOpsTokenLength = 16 // 内网运维令牌最小长度。

// validateOpsConfig 校验 Admin 内网接口的令牌和来源白名单。
func validateOpsConfig(cfg config.OpsConfig) error {
	if len(strings.TrimSpace(cfg.Token)) < minOpsTokenLength {
		return errors.Errorf("ops.token 长度不能小于 %d", minOpsTokenLength)
	}
	for _, item := range cfg.AllowedIPs {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if strings.Contains(item, "/") {
			prefix, err := netip.ParsePrefix(item)
			if err != nil {
				return errors.Wrapf(err, "ops.allowed_ips CIDR 非法: %s", item)
			}
			if !internalConfigPrefix(prefix) {
				return errors.Errorf("ops.allowed_ips 不能配置公网 CIDR: %s", item)
			}
			continue
		}
		addr, err := netip.ParseAddr(item)
		if err != nil {
			return errors.Wrapf(err, "ops.allowed_ips IP 非法: %s", item)
		}
		if !internalConfigAddr(addr) {
			return errors.Errorf("ops.allowed_ips 不能配置公网 IP: %s", item)
		}
	}
	return nil
}

// validateInternalServer 校验独立内网监听器，防止内网路由重新暴露到公网入口。
func validateInternalServer(public rest.RestConf, internal config.InternalServerConfig, mode string) error {
	host := strings.TrimSpace(internal.Host)
	if host == "" || host != internal.Host {
		return errors.Errorf("internal_server.host 必须配置且不能包含首尾空白")
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return errors.Wrap(err, "internal_server.host 必须使用明确 IP")
	}
	addr = addr.Unmap()
	if !addr.IsLoopback() && !addr.IsPrivate() && !addr.IsUnspecified() {
		return errors.Errorf("internal_server.host 只能使用回环或私有 IP")
	}
	if internal.Port <= 0 || internal.Port > 65535 {
		return errors.Errorf("internal_server.port 必须在 1-65535 之间")
	}
	if internal.Port == public.Port {
		return errors.Errorf("internal_server.port 不能与公网 HTTP 端口相同")
	}
	tlsEnabled, err := internalServerTLSEnabled(internal)
	if err != nil {
		return errors.Tag(err)
	}
	if productionMode(mode) {
		if addr.IsUnspecified() {
			return errors.Errorf("生产环境 internal_server.host 不能使用通配监听地址")
		}
		if !addr.IsLoopback() && !tlsEnabled {
			return errors.Errorf("生产环境跨主机 internal_server 必须启用 mTLS")
		}
	}
	return nil
}

// internalServerTLSEnabled 校验 mTLS 三个文件必须同时配置。
func internalServerTLSEnabled(cfg config.InternalServerConfig) (bool, error) {
	files := []string{
		strings.TrimSpace(cfg.CertFile),
		strings.TrimSpace(cfg.KeyFile),
		strings.TrimSpace(cfg.ClientCAFile),
	}
	configured := 0
	for _, file := range files {
		if file != "" {
			configured++
		}
	}
	if configured != 0 && configured != len(files) {
		return false, errors.Errorf("internal_server.cert_file、key_file、client_ca_file 必须同时配置")
	}
	return configured == len(files), nil
}

// validateAPIService 校验 Admin 到 API 内网监听器的连接与 mTLS 边界。
func validateAPIService(cfg config.APIServiceConfig, mode string) error {
	baseURL := strings.TrimSpace(cfg.InternalBaseURL)
	token := strings.TrimSpace(cfg.OpsToken)
	if baseURL == "" && token == "" && strings.TrimSpace(cfg.CAFile) == "" &&
		strings.TrimSpace(cfg.CertFile) == "" && strings.TrimSpace(cfg.KeyFile) == "" &&
		strings.TrimSpace(cfg.ServerName) == "" {
		return nil
	}
	if baseURL == "" || len(token) < minOpsTokenLength {
		return errors.Errorf("api_service.internal_base_url 必须配置且 ops_token 长度不能小于 %d", minOpsTokenLength)
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Path != "" && parsed.Path != "/") {
		return errors.Errorf("api_service.internal_base_url 配置不合法")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.Errorf("api_service.internal_base_url 仅支持 http/https")
	}
	tlsFiles := []string{
		strings.TrimSpace(cfg.CAFile),
		strings.TrimSpace(cfg.CertFile),
		strings.TrimSpace(cfg.KeyFile),
	}
	tlsConfigured := 0
	for _, file := range tlsFiles {
		if file != "" {
			tlsConfigured++
		}
	}
	if parsed.Scheme == "https" && tlsConfigured != len(tlsFiles) {
		return errors.Errorf("api_service 使用 HTTPS 时 ca_file、cert_file、key_file 必须同时配置")
	}
	if parsed.Scheme == "http" && tlsConfigured != 0 {
		return errors.Errorf("api_service 使用 HTTP 时不能配置 mTLS 文件")
	}
	if !productionMode(mode) {
		return nil
	}
	host, err := netip.ParseAddr(parsed.Hostname())
	if err == nil && host.Unmap().IsLoopback() {
		return nil
	}
	if parsed.Scheme != "https" {
		return errors.Errorf("生产环境跨主机 api_service 必须使用 HTTPS mTLS")
	}
	return nil
}

// internalConfigAddr 判断配置地址是否属于回环或私有地址。
func internalConfigAddr(addr netip.Addr) bool {
	addr = addr.Unmap()
	return addr.IsLoopback() || addr.IsPrivate()
}

// internalConfigPrefixes 收口允许的回环和私网网段；使用类型安全地址构造，避免在运行期配置校验中调用可能 panic 的 MustParsePrefix。
var internalConfigPrefixes = []netip.Prefix{
	netip.PrefixFrom(netip.AddrFrom4([4]byte{127, 0, 0, 0}), 8),
	netip.PrefixFrom(netip.AddrFrom4([4]byte{10, 0, 0, 0}), 8),
	netip.PrefixFrom(netip.AddrFrom4([4]byte{172, 16, 0, 0}), 12),
	netip.PrefixFrom(netip.AddrFrom4([4]byte{192, 168, 0, 0}), 16),
	netip.PrefixFrom(netip.AddrFrom16([16]byte{15: 1}), 128),
	netip.PrefixFrom(netip.AddrFrom16([16]byte{0xfc}), 7),
}

// internalConfigPrefix 确保 CIDR 的完整地址范围都落在回环或私网网段。
func internalConfigPrefix(prefix netip.Prefix) bool {
	prefix = prefix.Masked()
	for _, allowed := range internalConfigPrefixes {
		if prefix.Addr().BitLen() == allowed.Addr().BitLen() &&
			prefix.Bits() >= allowed.Bits() &&
			allowed.Contains(prefix.Addr()) {
			return true
		}
	}
	return false
}

// productionMode 判断配置是否属于生产运行模式。
func productionMode(mode string) bool {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "pro", "prod", "production":
		return true
	default:
		return false
	}
}
