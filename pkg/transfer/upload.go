package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"admin/helper"
	redislock "admin/internal/infra/redsync"
	"admin/pkg/storage"

	utils "github.com/Is999/go-utils"
	"github.com/Is999/go-utils/errors"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	// DefaultUploadSessionTTL 表示默认上传会话保留时间。
	DefaultUploadSessionTTL = 24 * time.Hour
	// uploadSessionLockTTL 表示单个上传会话写操作锁的过期时间。
	uploadSessionLockTTL = 30 * time.Second
	// uploadTempCleanupInterval 表示过期上传临时文件的机会清理间隔。
	uploadTempCleanupInterval = time.Hour
	// uploadTempCleanupLimit 表示单轮最多删除的过期上传临时文件数量。
	uploadTempCleanupLimit = 100
	// uploadTempDirLayout 表示上传临时文件按日期分目录的格式。
	uploadTempDirLayout = "20060102"
)

var (
	// ErrUploadSessionNotFound 表示上传会话不存在或已经过期。
	ErrUploadSessionNotFound = errors.New("上传会话不存在")
	// ErrUploadSessionStoreUnavailable 表示上传会话存储无法读取。
	ErrUploadSessionStoreUnavailable = errors.New("上传会话存储不可用")
	// uploadHashBufferPool 复用上传分片写入、跨设备复制和文件哈希计算缓冲，降低大文件链路的堆分配。
	uploadHashBufferPool = sync.Pool{
		New: func() any {
			// 上传链路复用 1MB 缓冲，避免大文件校验和跨设备复制反复分配临时切片。
			buffer := make([]byte, 1024*1024)
			return &buffer
		},
	}
)

// UploadSession 表示断点续传上传会话快照。
type UploadSession struct {
	UploadID       string                    `json:"uploadId"`               // 上传会话 ID
	OperatorID     int                       `json:"-"`                      // 发起上传的管理员 ID
	BizType        string                    `json:"bizType"`                // 业务类型
	FileName       string                    `json:"fileName"`               // 原始文件名
	StoredFileName string                    `json:"storedFileName"`         // 实际落库存储文件名（UUID）
	FileSize       int64                     `json:"fileSize"`               // 文件总大小
	ChunkSize      int64                     `json:"chunkSize"`              // 分片大小
	TotalChunks    int                       `json:"totalChunks"`            // 总分片数
	FileHash       string                    `json:"fileHash"`               // 文件摘要
	ContentType    string                    `json:"contentType"`            // 文件类型
	StoragePath    string                    `json:"storagePath"`            // 存储位置；本地上传为文件路径，直传为对象 key
	StorageType    string                    `json:"storageType"`            // 存储类型：local | s3
	ObjectKey      string                    `json:"objectKey"`              // 存储对象 key
	UploadMode     string                    `json:"uploadMode"`             // 上传模式：server | direct
	TempPath       string                    `json:"-"`                      // 临时文件路径
	Status         string                    `json:"status"`                 // 当前状态：uploading/pending/completed
	CreatedAt      string                    `json:"createdAt"`              // 创建时间
	UpdatedAt      string                    `json:"updatedAt"`              // 更新时间
	ExpiresAt      string                    `json:"expiresAt"`              // 过期时间
	UploadedChunks []int                     `json:"uploadedChunks"`         // 已上传分片下标
	AccessURL      string                    `json:"accessUrl"`              // 可直接访问的 URL
	DownloadURL    string                    `json:"downloadUrl"`            // 受控下载地址
	DirectUpload   *storage.DirectUploadPlan `json:"directUpload,omitempty"` // 前端直传预签名结果
}

// uploadSessionSnapshot 表示写入 Redis 的上传会话快照。
// 该结构在对外响应字段之外，额外持久化归属与临时路径信息，避免分片续传时状态丢失。
type uploadSessionSnapshot struct {
	UploadSession        // 对外上传会话字段
	OperatorID    int    `json:"operatorId"` // 发起上传的管理员 ID
	TempPath      string `json:"tempPath"`   // 上传中的临时文件路径
}

// InitUploadReq 表示初始化断点续传上传会话的参数。
type InitUploadReq struct {
	OperatorID  int    // 发起上传的管理员 ID
	BizType     string // 业务类型
	FileName    string // 文件名
	FileSize    int64  // 文件总大小
	ChunkSize   int64  // 分片大小
	FileHash    string // 文件摘要
	ContentType string // 文件内容类型
}

// LocalUploadManager 表示基于本地文件系统 + Redis 的断点续传上传管理器。
type LocalUploadManager struct {
	client               redis.UniversalClient // Redis 客户端
	rootDir              string                // 上传根目录
	sessionKeyFormat     string                // 上传会话 Redis key 模板
	chunksKeyFormat      string                // 上传分片状态 Redis key 模板
	fingerprintKeyFormat string                // 上传指纹复用索引 Redis key 模板
	objectIndexKeyFormat string                // 对象 key 反查上传会话 Redis key 模板
	ttl                  time.Duration         // 上传会话统一过期时间
	cleanupMu            sync.Mutex            // 串行化临时文件机会清理
	lastCleanup          time.Time             // 最近一次成功清理时间
}

// NewLocalUploadManager 创建本地断点续传上传管理器。
func NewLocalUploadManager(client redis.UniversalClient, rootDir string, sessionKeyFormat string, chunksKeyFormat string, fingerprintKeyFormat string, objectIndexKeyFormat string, ttl time.Duration) *LocalUploadManager {
	if ttl <= 0 {
		ttl = DefaultUploadSessionTTL
	}
	return &LocalUploadManager{
		client:               client,
		rootDir:              strings.TrimSpace(rootDir),
		sessionKeyFormat:     strings.TrimSpace(sessionKeyFormat),
		chunksKeyFormat:      strings.TrimSpace(chunksKeyFormat),
		fingerprintKeyFormat: strings.TrimSpace(fingerprintKeyFormat),
		objectIndexKeyFormat: strings.TrimSpace(objectIndexKeyFormat),
		ttl:                  ttl,
	}
}

// Init 创建新的断点续传上传会话。
func (m *LocalUploadManager) Init(ctx context.Context, req InitUploadReq) (*UploadSession, error) {
	if err := m.validate(); err != nil {
		return nil, errors.Tag(err)
	}
	fileName := strings.TrimSpace(req.FileName)
	if fileName == "" {
		return nil, errors.Errorf("文件名不能为空")
	}
	if req.FileSize <= 0 {
		return nil, errors.Errorf("文件大小必须大于 0")
	}
	if req.ChunkSize <= 0 {
		return nil, errors.Errorf("分片大小必须大于 0")
	}
	if err := m.cleanupExpiredTempFiles(ctx, time.Now()); err != nil {
		return nil, errors.Tag(err)
	}
	fingerprint := buildUploadFingerprint(req.OperatorID, strings.TrimSpace(req.BizType), fileName, req.FileSize, strings.TrimSpace(req.FileHash))
	if fingerprint != "" {
		// 同一管理员重复上传同一文件时优先复用现有会话，避免跨管理员泄露 uploadId 与上传状态。
		if session, err := m.loadReusableSession(ctx, fingerprint, req.OperatorID); err == nil && session != nil {
			return session, nil
		}
	}

	now := time.Now()
	uploadID := uuid.NewString()
	totalChunks := int((req.FileSize + req.ChunkSize - 1) / req.ChunkSize)
	if totalChunks <= 0 {
		totalChunks = 1
	}
	// 目录路径只使用清洗后的业务段，避免工具包被复用时因 bizType 注入路径穿越。
	safeBizType := sanitizeUploadSegment(req.BizType)
	storageDir := filepath.Join(m.rootDir, "completed", safeBizType)
	tempDir := filepath.Join(m.rootDir, "uploading", safeBizType, now.Format(uploadTempDirLayout))
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		return nil, errors.Wrap(err, "创建上传结果目录失败")
	}
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return nil, errors.Wrap(err, "创建上传临时目录失败")
	}

	// 初始化阶段先写入上传元数据，后续每个分片只更新状态和最终落地路径。
	session := &UploadSession{
		UploadID:       uploadID,
		OperatorID:     req.OperatorID,
		BizType:        strings.TrimSpace(req.BizType),
		FileName:       fileName,
		StoredFileName: storage.BuildStoredFileName(uploadID, fileName),
		FileSize:       req.FileSize,
		ChunkSize:      req.ChunkSize,
		TotalChunks:    totalChunks,
		FileHash:       strings.TrimSpace(req.FileHash),
		ContentType:    strings.TrimSpace(req.ContentType),
		StoragePath:    filepath.Join(storageDir, storage.BuildStoredFileName(uploadID, fileName)),
		UploadMode:     storage.UploadModeServer,
		TempPath:       filepath.Join(tempDir, uploadID+".part"),
		Status:         "uploading",
		CreatedAt:      formatDateTime(now),
		UpdatedAt:      formatDateTime(now),
		ExpiresAt:      formatDateTime(now.Add(m.ttl)),
	}
	if err := m.PersistSession(ctx, session); err != nil {
		return nil, errors.Tag(err)
	}
	if fingerprint != "" {
		if err := m.client.Set(ctx, m.fingerprintKey(fingerprint), uploadID, m.ttl).Err(); err != nil {
			return nil, errors.Wrap(err, "保存上传会话指纹索引失败")
		}
	}
	return session, nil
}

// Status 查询上传会话当前状态。
func (m *LocalUploadManager) Status(ctx context.Context, uploadID string) (*UploadSession, error) {
	session, err := m.loadSession(ctx, uploadID)
	if err != nil {
		return nil, errors.Tag(err)
	}
	chunks, err := m.uploadedChunks(ctx, uploadID)
	if err != nil {
		return nil, errors.Tag(err)
	}
	session.UploadedChunks = chunks
	return session, nil
}

// GetSession 查询上传会话原始信息，供业务层做权限控制和下载输出。
func (m *LocalUploadManager) GetSession(ctx context.Context, uploadID string) (*UploadSession, error) {
	return m.loadSession(ctx, uploadID)
}

// HasSession 判断上传会话是否仍在有效期内，供持久化对象清理任务避免提前删除。
func (m *LocalUploadManager) HasSession(ctx context.Context, uploadID string) (bool, error) {
	if err := m.validate(); err != nil {
		return false, errors.Tag(err)
	}
	uploadID = strings.TrimSpace(uploadID)
	if uploadID == "" {
		return false, nil
	}
	exists, err := m.client.Exists(ctx, m.sessionKey(uploadID)).Result()
	if err != nil {
		return false, errors.Wrap(err, "检查上传会话失败")
	}
	return exists > 0, nil
}

// DeleteSession 删除已经被业务消费的上传会话及其精确索引，防止同一文件复用失效对象。
func (m *LocalUploadManager) DeleteSession(ctx context.Context, session *UploadSession) error {
	if err := m.validate(); err != nil {
		return errors.Tag(err)
	}
	if session == nil || strings.TrimSpace(session.UploadID) == "" {
		return errors.Errorf("上传会话不能为空")
	}
	fingerprint := buildUploadFingerprint(
		session.OperatorID,
		session.BizType,
		session.FileName,
		session.FileSize,
		session.FileHash,
	)
	keys := []string{
		m.sessionKey(session.UploadID),
		m.chunksKey(session.UploadID),
	}
	if strings.TrimSpace(session.ObjectKey) != "" {
		keys = append(keys, m.objectIndexKey(session.ObjectKey))
	}
	if fingerprint != "" {
		keys = append(keys, m.fingerprintKey(fingerprint))
	}
	var deleteErr error
	for _, key := range keys {
		if err := m.client.Del(ctx, key).Err(); err != nil {
			deleteErr = errors.Join(deleteErr, errors.Wrapf(err, "删除上传会话缓存失败 key=%s", key))
		}
	}
	return deleteErr
}

// DeleteCompletedFile 删除服务端中转上传遗留的 completed 文件，对文件不存在保持幂等。
func (m *LocalUploadManager) DeleteCompletedFile(bizType string, objectKey string) error {
	if err := m.validate(); err != nil {
		return errors.Tag(err)
	}
	bizType = strings.TrimSpace(bizType)
	fileName := strings.TrimSpace(filepath.Base(filepath.FromSlash(strings.TrimSpace(objectKey))))
	if bizType == "" || fileName == "" || fileName == "." {
		return errors.Errorf("上传完成文件定位参数不能为空")
	}
	filePath := filepath.Join(m.rootDir, "completed", sanitizeUploadSegment(bizType), fileName)
	if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.Wrap(err, "删除上传完成文件失败")
	}
	return nil
}

// PersistSession 保存或覆盖上传会话，供直传等扩展场景复用统一状态存储。
func (m *LocalUploadManager) PersistSession(ctx context.Context, session *UploadSession) error {
	return m.saveSession(ctx, session)
}

// FindSessionByObjectKey 根据统一存储对象 key 反查上传会话，便于导入等场景复用原有权限边界。
func (m *LocalUploadManager) FindSessionByObjectKey(ctx context.Context, objectKey string) (*UploadSession, error) {
	if err := m.validate(); err != nil {
		return nil, errors.Tag(err)
	}
	objectKey = strings.TrimSpace(objectKey)
	if objectKey == "" {
		return nil, errors.Errorf("对象 key 不能为空")
	}
	uploadID, err := m.client.Get(ctx, m.objectIndexKey(objectKey)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrUploadSessionNotFound
		}
		return nil, errors.Wrap(err, "读取对象反查索引失败")
	}
	session, err := m.loadSession(ctx, uploadID)
	if err != nil {
		return nil, errors.Tag(err)
	}
	// 反查索引可能由一次未提交完成的写入留下，必须以主会话快照中的对象 key 为准。
	if strings.TrimSpace(session.ObjectKey) != objectKey {
		return nil, errors.Errorf("上传对象反查索引已失效")
	}
	return session, nil
}

// UploadChunk 写入一个分片。
func (m *LocalUploadManager) UploadChunk(ctx context.Context, uploadID string, chunkIndex int, body io.Reader) (*UploadSession, error) {
	if body == nil {
		return nil, errors.Errorf("上传分片内容不能为空")
	}
	session, err := m.loadSession(ctx, uploadID)
	if err != nil {
		return nil, errors.Tag(err)
	}
	if session.Status == "completed" {
		return session, nil
	}
	if chunkIndex < 0 || chunkIndex >= session.TotalChunks {
		return nil, errors.Errorf("分片下标超出范围")
	}
	if uploaded, err := m.isChunkUploaded(ctx, uploadID, chunkIndex); err != nil {
		return nil, errors.Tag(err)
	} else if uploaded {
		return m.Status(ctx, uploadID)
	}

	var result *UploadSession
	if err := m.withUploadChunkLock(ctx, uploadID, chunkIndex, func(lockCtx context.Context) error {
		current, err := m.loadSession(lockCtx, uploadID)
		if err != nil {
			return errors.Tag(err)
		}
		if current.Status == "completed" {
			result = current
			return nil
		}
		if uploaded, err := m.isChunkUploaded(lockCtx, uploadID, chunkIndex); err != nil {
			return errors.Tag(err)
		} else if uploaded {
			result, err = m.Status(ctx, uploadID)
			return errors.Tag(err)
		}
		currentExpectedSize := expectedChunkSize(current, chunkIndex)
		if err := m.writeChunkFileFromReader(lockCtx, current, chunkIndex, currentExpectedSize, body); err != nil {
			return errors.Tag(err)
		}
		marked, err := m.withUploadLock(lockCtx, uploadID, func(lockCtx context.Context) (*UploadSession, error) {
			current, err = m.loadSession(lockCtx, uploadID)
			if err != nil {
				return nil, errors.Tag(err)
			}
			if current.Status == "completed" {
				return current, nil
			}
			if chunkIndex < 0 || chunkIndex >= current.TotalChunks {
				return nil, errors.Errorf("分片下标超出范围")
			}
			if err := m.markChunkUploadedLocked(lockCtx, current, uploadID, chunkIndex); err != nil {
				return nil, errors.Tag(err)
			}
			return m.Status(lockCtx, uploadID)
		})
		if err != nil {
			return errors.Tag(err)
		}
		result = marked
		return nil
	}); err != nil {
		return nil, errors.Tag(err)
	}
	if result != nil {
		return result, nil
	}
	return m.Status(ctx, uploadID)
}

// writeChunkFileFromReader 在分片级锁内流式写入单个分片。
// 不同分片使用 File.WriteAt 并行落盘；同一分片串行化，避免重复上传时互相覆盖。
func (m *LocalUploadManager) writeChunkFileFromReader(ctx context.Context, session *UploadSession, chunkIndex int, expectedSize int64, body io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(session.TempPath), 0o755); err != nil {
		return errors.Wrap(err, "创建上传临时目录失败")
	}
	file, err := os.OpenFile(session.TempPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return errors.Wrap(err, "打开上传临时文件失败")
	}
	defer file.Close()

	if info, statErr := file.Stat(); statErr != nil {
		return errors.Wrap(statErr, "读取上传临时文件状态失败")
	} else if info.Size() != session.FileSize {
		if err := file.Truncate(session.FileSize); err != nil {
			return errors.Wrap(err, "预分配上传临时文件失败")
		}
	}
	offset := int64(chunkIndex) * session.ChunkSize
	written, err := writeFixedChunk(ctx, file, offset, expectedSize, body)
	if err != nil {
		return errors.Tag(err)
	}
	if written != expectedSize {
		return errors.Errorf("分片大小不正确")
	}
	return nil
}

// markChunkUploadedLocked 在会话锁内记录分片完成状态，并续期上传会话。
// 文件写入已经在分片级锁内完成，这里只做短事务 Redis 状态更新，减少同文件多分片互相等待。
func (m *LocalUploadManager) markChunkUploadedLocked(ctx context.Context, session *UploadSession, uploadID string, chunkIndex int) error {
	if session == nil {
		return errors.Errorf("上传会话不能为空")
	}
	if uploaded, err := m.isChunkUploaded(ctx, uploadID, chunkIndex); err != nil {
		return errors.Tag(err)
	} else if uploaded {
		return m.refreshTTL(ctx, uploadID)
	}
	if err := m.client.SAdd(ctx, m.chunksKey(uploadID), chunkIndex).Err(); err != nil {
		return errors.Wrap(err, "记录上传分片状态失败")
	}
	if err := m.refreshTTL(ctx, uploadID); err != nil {
		return errors.Tag(err)
	}
	return nil
}

// writeFixedChunk 从请求体读取指定大小内容并写入目标偏移，额外数据或提前 EOF 都视为分片大小错误。
func writeFixedChunk(ctx context.Context, file *os.File, offset int64, expectedSize int64, body io.Reader) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if file == nil || body == nil {
		return 0, errors.Errorf("上传分片写入参数不能为空")
	}
	if expectedSize < 0 {
		return 0, errors.Errorf("分片大小不正确")
	}
	bufferPtr := uploadHashBufferPool.Get().(*[]byte)
	defer uploadHashBufferPool.Put(bufferPtr)
	buffer := *bufferPtr
	writtenTotal := int64(0)
	for writtenTotal < expectedSize {
		if err := ctx.Err(); err != nil {
			return writtenTotal, errors.Tag(err)
		}
		readSize := len(buffer)
		remaining := expectedSize - writtenTotal
		if int64(readSize) > remaining {
			readSize = int(remaining)
		}
		readCount, readErr := body.Read(buffer[:readSize])
		if readCount > 0 {
			written, writeErr := file.WriteAt(buffer[:readCount], offset+writtenTotal)
			if writeErr != nil {
				return writtenTotal, errors.Wrap(writeErr, "写入上传分片失败")
			}
			if written != readCount {
				return writtenTotal, errors.Wrap(io.ErrShortWrite, "写入上传分片不完整")
			}
			writtenTotal += int64(written)
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				if writtenTotal == expectedSize {
					break
				}
				return writtenTotal, errors.Errorf("分片大小不正确")
			}
			return writtenTotal, errors.Wrap(readErr, "读取上传分片失败")
		}
		if readCount == 0 {
			return writtenTotal, errors.Wrap(io.ErrNoProgress, "读取上传分片无进展")
		}
	}
	var extra [1]byte
	readCount, readErr := body.Read(extra[:])
	if readCount > 0 {
		return writtenTotal, errors.Errorf("分片大小不正确")
	}
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return writtenTotal, errors.Wrap(readErr, "读取上传分片失败")
	}
	return writtenTotal, nil
}

// Complete 校验所有分片，并在文件安全校验通过后完成会话。
func (m *LocalUploadManager) Complete(ctx context.Context, uploadID string, validateFile func(context.Context, *UploadSession, string) error) (*UploadSession, error) {
	if validateFile == nil {
		return nil, errors.Errorf("上传完成前校验不能为空")
	}
	return m.withUploadLock(ctx, uploadID, func(lockCtx context.Context) (*UploadSession, error) {
		return m.completeLocked(lockCtx, uploadID, validateFile)
	})
}

// completeLocked 在会话锁内校验所有分片并完成上传。
func (m *LocalUploadManager) completeLocked(ctx context.Context, uploadID string, validateFile func(context.Context, *UploadSession, string) error) (*UploadSession, error) {
	session, err := m.loadSession(ctx, uploadID)
	if err != nil {
		return nil, errors.Tag(err)
	}
	if strings.EqualFold(strings.TrimSpace(session.Status), "completed") {
		return session, nil
	}
	chunkCount, err := m.uploadedChunkCount(ctx, uploadID)
	if err != nil {
		return nil, errors.Tag(err)
	}
	if chunkCount != int64(session.TotalChunks) {
		return nil, errors.Errorf("仍有分片未上传完成")
	}
	chunks, err := m.uploadedChunks(ctx, uploadID)
	if err != nil {
		return nil, errors.Tag(err)
	}
	if len(chunks) != session.TotalChunks {
		return nil, errors.Errorf("仍有分片未上传完成")
	}
	if err := os.MkdirAll(filepath.Dir(session.StoragePath), 0o755); err != nil {
		return nil, errors.Wrap(err, "创建上传结果目录失败")
	}
	if !fileExists(session.TempPath) && fileExists(session.StoragePath) {
		// 如果进程在 Rename 成功但 Redis 状态持久化前退出，下一次 Complete 直接校验最终文件并修复会话状态。
		if err := verifyUploadedFileHash(session.StoragePath, session.FileHash); err != nil {
			return nil, errors.Tag(err)
		}
		if err := validateFile(ctx, session, session.StoragePath); err != nil {
			return nil, errors.Tag(err)
		}
		return m.markSessionCompleted(ctx, session, chunks)
	}
	if sameDevice, ok := pathsOnSameDevice(session.TempPath, filepath.Dir(session.StoragePath)); ok && !sameDevice {
		if err := validateFile(ctx, session, session.TempPath); err != nil {
			return nil, errors.Tag(err)
		}
		// 已知上传临时目录和完成目录跨设备时，复制过程中同步计算摘要，避免先校验再复制造成大文件双读。
		if err := copyUploadedFileAcrossDevices(session.TempPath, session.StoragePath, session.FileHash); err != nil {
			return nil, errors.Wrap(err, "跨设备完成上传文件失败")
		}
		return m.markSessionCompleted(ctx, session, chunks)
	}
	if err := verifyUploadedFileHash(session.TempPath, session.FileHash); err != nil {
		return nil, errors.Tag(err)
	}
	if err := validateFile(ctx, session, session.TempPath); err != nil {
		return nil, errors.Tag(err)
	}
	if err := os.Rename(session.TempPath, session.StoragePath); err != nil {
		if errors.Is(err, os.ErrNotExist) && fileExists(session.StoragePath) {
			if hashErr := verifyUploadedFileHash(session.StoragePath, session.FileHash); hashErr != nil {
				return nil, errors.Tag(hashErr)
			}
			if validateErr := validateFile(ctx, session, session.StoragePath); validateErr != nil {
				return nil, errors.Tag(validateErr)
			}
			return m.markSessionCompleted(ctx, session, chunks)
		}
		if !helper.IsCrossDeviceRenameError(err) {
			return nil, errors.Wrap(err, "完成上传文件失败")
		}
		if copyErr := copyUploadedFileAcrossDevices(session.TempPath, session.StoragePath, ""); copyErr != nil {
			return nil, errors.Wrap(copyErr, "跨设备完成上传文件失败")
		}
	}
	return m.markSessionCompleted(ctx, session, chunks)
}

// markSessionCompleted 在会话锁内标记上传完成，并刷新 Redis 会话快照。
func (m *LocalUploadManager) markSessionCompleted(ctx context.Context, session *UploadSession, chunks []int) (*UploadSession, error) {
	session.Status = "completed"
	session.UpdatedAt = formatDateTime(time.Now())
	session.UploadedChunks = chunks
	if err := m.PersistSession(ctx, session); err != nil {
		return nil, errors.Tag(err)
	}
	return session, nil
}

// withUploadLock 为单个上传会话串行化写操作，避免分片写入与完成提交互相踩踏。
func (m *LocalUploadManager) withUploadLock(ctx context.Context, uploadID string, fn func(context.Context) (*UploadSession, error)) (*UploadSession, error) {
	if fn == nil {
		return nil, errors.Errorf("上传会话锁回调不能为空")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := m.validate(); err != nil {
		return nil, errors.Tag(err)
	}
	uploadID = strings.TrimSpace(uploadID)
	if uploadID == "" {
		return nil, errors.Errorf("uploadId 不能为空")
	}
	var result *UploadSession
	err := m.withRedisLock(ctx, m.lockKey(uploadID), "上传会话锁", func(lockCtx context.Context) error {
		var err error
		result, err = fn(lockCtx)
		if err != nil {
			return errors.Wrapf(err, "上传会话锁回调失败 upload_id=%s", uploadID)
		}
		return nil
	})
	return result, errors.Tag(err)
}

// withUploadChunkLock 为单个上传分片串行化文件写入，允许同一文件的不同分片并行。
func (m *LocalUploadManager) withUploadChunkLock(ctx context.Context, uploadID string, chunkIndex int, fn func(context.Context) error) error {
	if fn == nil {
		return errors.Errorf("上传分片锁回调不能为空")
	}
	uploadID = strings.TrimSpace(uploadID)
	if uploadID == "" {
		return errors.Errorf("uploadId 不能为空")
	}
	if chunkIndex < 0 {
		return errors.Errorf("分片下标超出范围")
	}
	return m.withRedisLock(ctx, m.chunkLockKey(uploadID, chunkIndex), "上传分片锁", fn)
}

// withRedisLock 使用项目统一 Redis owner 租约封装实现短时互斥锁。
// 会话锁只保护状态提交；分片锁只保护同一 chunk 的文件覆盖，锁丢失时通过 lockCtx 通知长耗时写入尽快退出。
func (m *LocalUploadManager) withRedisLock(ctx context.Context, lockKey string, lockName string, fn func(context.Context) error) error {
	if fn == nil {
		return errors.Errorf("%s回调不能为空", lockName)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := m.validate(); err != nil {
		return errors.Tag(err)
	}
	lockKey = strings.TrimSpace(lockKey)
	if lockKey == "" {
		return errors.Errorf("%s key 不能为空", lockName)
	}
	// lockErr 保留 Redis 锁加锁、续期、释放或业务回调错误，统一追加锁名称便于排障。
	lockErr := redislock.WithLock(ctx, m.client, lockKey, uploadSessionLockTTL, func(lockCtx context.Context) error {
		return fn(lockCtx)
	})
	if lockErr != nil {
		return errors.Wrapf(lockErr, "%s执行失败", lockName)
	}
	return nil
}

// validate 校验上传管理器基础依赖是否完整。
func (m *LocalUploadManager) validate() error {
	if m == nil || m.client == nil {
		return errors.Errorf("上传管理器未初始化")
	}
	if strings.TrimSpace(m.rootDir) == "" {
		return errors.Errorf("上传根目录不能为空")
	}
	if strings.TrimSpace(m.sessionKeyFormat) == "" || strings.TrimSpace(m.chunksKeyFormat) == "" {
		return errors.Errorf("上传 Redis key 模板不能为空")
	}
	if strings.TrimSpace(m.objectIndexKeyFormat) == "" {
		return errors.Errorf("上传对象反查索引 key 模板不能为空")
	}
	return nil
}

// cleanupExpiredTempFiles 小批量删除超过会话 TTL 的中断上传文件。
// 新上传按日期分目录，遍历时可跳过仍在有效期内的整日目录，避免扫描活跃文件。
func (m *LocalUploadManager) cleanupExpiredTempFiles(ctx context.Context, now time.Time) error {
	m.cleanupMu.Lock()
	defer m.cleanupMu.Unlock()
	if !m.lastCleanup.IsZero() && now.Sub(m.lastCleanup) < uploadTempCleanupInterval {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	uploadRoot := filepath.Join(m.rootDir, "uploading")
	cutoff := now.Add(-m.ttl)
	removed := 0
	err := filepath.WalkDir(uploadRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return errors.Tag(err)
		}
		if entry.IsDir() {
			relativePath, err := filepath.Rel(uploadRoot, path)
			if err != nil {
				return errors.Tag(err)
			}
			parts := strings.Split(relativePath, string(os.PathSeparator))
			if len(parts) == 2 {
				date, err := time.ParseInLocation(uploadTempDirLayout, parts[1], now.Location())
				if err == nil && date.After(cutoff) {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if removed >= uploadTempCleanupLimit {
			return filepath.SkipAll
		}
		if !strings.HasSuffix(strings.ToLower(entry.Name()), ".part") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return errors.Wrap(err, "读取上传临时文件状态失败")
		}
		if info.ModTime().After(cutoff) {
			return nil
		}
		uploadID := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
		exists, err := m.client.Exists(ctx, m.sessionKey(uploadID)).Result()
		if err != nil {
			return errors.Wrap(err, "检查上传临时文件会话失败")
		}
		if exists > 0 {
			return nil
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return errors.Wrap(err, "删除过期上传临时文件失败")
		}
		removed++
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.Wrap(err, "清理过期上传临时文件失败")
	}
	m.lastCleanup = now
	return nil
}

// loadSession 从 Redis 读取上传会话，并恢复内部派生字段。
func (m *LocalUploadManager) loadSession(ctx context.Context, uploadID string) (*UploadSession, error) {
	if m == nil || m.client == nil {
		return nil, errors.Join(ErrUploadSessionStoreUnavailable, errors.Errorf("上传管理器未初始化"))
	}
	if err := m.validate(); err != nil {
		return nil, errors.Tag(err)
	}
	uploadID = strings.TrimSpace(uploadID)
	if uploadID == "" {
		return nil, errors.Errorf("uploadId 不能为空")
	}
	body, err := m.client.Get(ctx, m.sessionKey(uploadID)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrUploadSessionNotFound
		}
		return nil, errors.Join(ErrUploadSessionStoreUnavailable, errors.Wrap(err, "读取上传会话失败"))
	}
	var snapshot uploadSessionSnapshot
	if err := json.Unmarshal([]byte(body), &snapshot); err != nil {
		return nil, errors.Wrap(err, "解析上传会话失败")
	}
	session := snapshot.UploadSession
	session.OperatorID = snapshot.OperatorID
	session.TempPath = strings.TrimSpace(snapshot.TempPath)
	m.restoreSessionDerivedFields(&session)
	return &session, nil
}

// saveSession 把上传会话快照写入 Redis；对象索引和分片 TTL 成功后才提交主快照。
func (m *LocalUploadManager) saveSession(ctx context.Context, session *UploadSession) error {
	if session == nil {
		return errors.Errorf("上传会话不能为空")
	}
	snapshot := uploadSessionSnapshot{
		UploadSession: *session,
		OperatorID:    session.OperatorID,
		TempPath:      strings.TrimSpace(session.TempPath),
	}
	body, err := json.Marshal(snapshot)
	if err != nil {
		return errors.Wrap(err, "序列化上传会话失败")
	}
	if strings.TrimSpace(session.ObjectKey) != "" {
		// 跨槽 key 无法使用单条 Lua 原子提交，先写可校验的辅助索引，最后写主会话快照作为提交点。
		if err := m.client.Set(ctx, m.objectIndexKey(session.ObjectKey), session.UploadID, m.ttl).Err(); err != nil {
			return errors.Wrap(err, "保存上传对象反查索引失败")
		}
	}
	if err := m.client.Expire(ctx, m.chunksKey(session.UploadID), m.ttl).Err(); err != nil {
		return errors.Wrap(err, "刷新上传分片状态有效期失败")
	}
	if err := m.client.Set(ctx, m.sessionKey(session.UploadID), body, m.ttl).Err(); err != nil {
		return errors.Wrap(err, "保存上传会话失败")
	}
	return nil
}

// uploadedChunks 读取并排序已上传分片下标。
func (m *LocalUploadManager) uploadedChunks(ctx context.Context, uploadID string) ([]int, error) {
	values, err := m.client.SMembers(ctx, m.chunksKey(uploadID)).Result()
	if err != nil {
		return nil, errors.Wrap(err, "读取上传分片状态失败")
	}
	result := make([]int, 0, len(values))
	for _, value := range values {
		index, convErr := strconv.Atoi(strings.TrimSpace(value))
		if convErr != nil {
			continue
		}
		result = append(result, index)
	}
	sort.Ints(result)
	return result, nil
}

// uploadedChunkCount 只读取已上传分片数量，用于 Complete 快速判断是否还有缺口。
func (m *LocalUploadManager) uploadedChunkCount(ctx context.Context, uploadID string) (int64, error) {
	count, err := m.client.SCard(ctx, m.chunksKey(uploadID)).Result()
	if err != nil {
		return 0, errors.Wrap(err, "读取上传分片数量失败")
	}
	return count, nil
}

// isChunkUploaded 精确判断某个分片是否已经完成，避免重复分片再次覆盖临时文件。
func (m *LocalUploadManager) isChunkUploaded(ctx context.Context, uploadID string, chunkIndex int) (bool, error) {
	uploaded, err := m.client.SIsMember(ctx, m.chunksKey(uploadID), chunkIndex).Result()
	if err != nil {
		return false, errors.Wrap(err, "读取上传分片完成状态失败")
	}
	return uploaded, nil
}

// refreshTTL 刷新上传会话、分片集合和会话更新时间。
func (m *LocalUploadManager) refreshTTL(ctx context.Context, uploadID string) error {
	if err := m.client.Expire(ctx, m.sessionKey(uploadID), m.ttl).Err(); err != nil {
		return errors.Wrap(err, "续期上传会话失败")
	}
	if err := m.client.Expire(ctx, m.chunksKey(uploadID), m.ttl).Err(); err != nil {
		return errors.Wrap(err, "续期上传分片状态失败")
	}
	session, err := m.loadSession(ctx, uploadID)
	if err != nil {
		return errors.Tag(err)
	}
	session.UpdatedAt = formatDateTime(time.Now())
	return m.PersistSession(ctx, session)
}

// loadReusableSession 按上传指纹读取可复用会话，并强制校验会话归属与文件状态仍然有效。
func (m *LocalUploadManager) loadReusableSession(ctx context.Context, fingerprint string, operatorID int) (*UploadSession, error) {
	if strings.TrimSpace(m.fingerprintKeyFormat) == "" || strings.TrimSpace(fingerprint) == "" {
		return nil, nil
	}
	uploadID, err := m.client.Get(ctx, m.fingerprintKey(fingerprint)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "读取上传会话指纹索引失败")
	}
	session, err := m.Status(ctx, uploadID)
	if err != nil {
		_ = m.client.Del(ctx, m.fingerprintKey(fingerprint)).Err()
		return nil, nil
	}
	// 复用会话必须仍归属于当前管理员，避免同一文件摘要导致跨管理员串用 uploadId。
	if session.OperatorID > 0 && operatorID > 0 && session.OperatorID != operatorID {
		return nil, nil
	}
	if session.Status == "completed" && strings.TrimSpace(session.ObjectKey) == "" && !fileExists(session.StoragePath) {
		_ = m.client.Del(ctx, m.fingerprintKey(fingerprint)).Err()
		return nil, nil
	}
	if session.Status == "uploading" && !fileExists(session.TempPath) {
		_ = m.client.Del(ctx, m.fingerprintKey(fingerprint)).Err()
		return nil, nil
	}
	return session, nil
}

// restoreSessionDerivedFields 为已存在或裁剪后的会话恢复临时路径等派生字段，保证续传链路完整。
func (m *LocalUploadManager) restoreSessionDerivedFields(session *UploadSession) {
	if session == nil {
		return
	}
	if strings.TrimSpace(session.TempPath) == "" &&
		strings.EqualFold(strings.TrimSpace(session.UploadMode), storage.UploadModeServer) &&
		strings.TrimSpace(session.UploadID) != "" {
		tempDir := filepath.Join(m.rootDir, "uploading", sanitizeUploadSegment(session.BizType))
		if createdAt, err := time.Parse(time.DateTime, session.CreatedAt); err == nil {
			tempDir = filepath.Join(tempDir, createdAt.Format(uploadTempDirLayout))
		}
		session.TempPath = filepath.Join(tempDir, session.UploadID+".part")
	}
}

// sessionKey 返回上传会话 Redis key。
func (m *LocalUploadManager) sessionKey(uploadID string) string {
	return strings.TrimSpace(strings.ReplaceAll(m.sessionKeyFormat, "%s", uploadID))
}

// chunksKey 返回上传分片集合 Redis key。
func (m *LocalUploadManager) chunksKey(uploadID string) string {
	return strings.TrimSpace(strings.ReplaceAll(m.chunksKeyFormat, "%s", uploadID))
}

// fingerprintKey 返回上传指纹复用索引 Redis key。
func (m *LocalUploadManager) fingerprintKey(fingerprint string) string {
	return strings.TrimSpace(strings.ReplaceAll(m.fingerprintKeyFormat, "%s", fingerprint))
}

// objectIndexKey 返回对象 key 反查上传会话的 Redis key。
func (m *LocalUploadManager) objectIndexKey(objectKey string) string {
	// objectKey 先做 SHA-256 摘要，避免完整对象路径直接进入 Redis key。
	return strings.TrimSpace(strings.ReplaceAll(m.objectIndexKeyFormat, "%s", utils.SHA256(strings.TrimSpace(objectKey))))
}

// lockKey 返回单个上传会话写操作锁 Redis key。
func (m *LocalUploadManager) lockKey(uploadID string) string {
	return m.sessionKey(uploadID) + ":lock"
}

// chunkLockKey 返回单个上传分片文件写入锁 Redis key。
func (m *LocalUploadManager) chunkLockKey(uploadID string, chunkIndex int) string {
	return m.sessionKey(uploadID) + ":chunk:" + strconv.Itoa(chunkIndex) + ":lock"
}

// expectedChunkSize 计算指定分片应接收的字节数。
func expectedChunkSize(session *UploadSession, chunkIndex int) int64 {
	if session == nil {
		return 0
	}
	if chunkIndex == session.TotalChunks-1 {
		remaining := session.FileSize - int64(chunkIndex)*session.ChunkSize
		if remaining > 0 {
			return remaining
		}
	}
	return session.ChunkSize
}

// buildUploadFingerprint 计算上传会话复用指纹，并把管理员 ID 纳入摘要避免跨账号串用上传会话。
func buildUploadFingerprint(operatorID int, bizType string, fileName string, fileSize int64, fileHash string) string {
	fileHash = strings.TrimSpace(fileHash)
	if fileHash == "" {
		return ""
	}
	return utils.SHA256(strings.Join([]string{
		strconv.Itoa(operatorID),
		strings.TrimSpace(bizType),
		strings.TrimSpace(fileName),
		strconv.FormatInt(fileSize, 10),
		fileHash,
	}, "|"))
}

// sanitizeUploadSegment 清洗上传临时目录业务段，只保留安全文件名字符。
func sanitizeUploadSegment(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "common"
	}
	builder := strings.Builder{}
	for _, ch := range value {
		switch {
		case ch >= 'a' && ch <= 'z':
			builder.WriteRune(ch)
		case ch >= 'A' && ch <= 'Z':
			builder.WriteRune(ch + ('a' - 'A'))
		case ch >= '0' && ch <= '9':
			builder.WriteRune(ch)
		case ch == '-', ch == '_':
			builder.WriteRune(ch)
		}
	}
	if builder.Len() == 0 {
		return "common"
	}
	return builder.String()
}

// verifyUploadedFileHash 校验上传文件 SHA-256 摘要，expectedHash 为空时跳过。
func verifyUploadedFileHash(filePath string, expectedHash string) error {
	expectedHash = strings.ToLower(strings.TrimSpace(expectedHash))
	if expectedHash == "" {
		return nil
	}
	actualHash, err := calculateUploadedFileHash(filePath)
	if err != nil {
		return errors.Tag(err)
	}
	if actualHash != expectedHash {
		return errors.Errorf("上传文件完整性校验失败")
	}
	return nil
}

// calculateUploadedFileHash 顺序读取本地文件并计算 SHA-256 摘要。
// 断点续传支持乱序 WriteAt，整文件摘要无法由分片摘要安全合成，因此 Complete 阶段仍需要一次顺序校验。
func calculateUploadedFileHash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", errors.Wrap(err, "打开待校验上传文件失败")
	}
	defer file.Close()

	// digest 接收文件顺序字节，用于和前端声明的整文件 SHA-256 比对。
	digest := sha256.New()
	bufferPtr := uploadHashBufferPool.Get().(*[]byte)
	defer uploadHashBufferPool.Put(bufferPtr)
	if _, err := io.CopyBuffer(digest, file, *bufferPtr); err != nil {
		return "", errors.Wrap(err, "计算上传文件摘要失败")
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// copyUploadedFileAcrossDevices 在上传临时目录和完成目录跨挂载卷时复制文件并提交。
// 目标目录内仍使用临时文件承接复制结果，最后一次本地 rename 防止读到半截文件。
func copyUploadedFileAcrossDevices(srcPath string, dstPath string, expectedHash string) error {
	expectedHash = strings.ToLower(strings.TrimSpace(expectedHash))
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return errors.Wrap(err, "打开跨设备上传源文件失败")
	}
	defer srcFile.Close()

	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return errors.Wrap(err, "创建跨设备上传目标目录失败")
	}
	dstFile, err := os.CreateTemp(filepath.Dir(dstPath), ".upload-complete-*")
	if err != nil {
		return errors.Wrap(err, "创建跨设备上传目标临时文件失败")
	}
	dstTempPath := dstFile.Name()
	defer func() {
		_ = os.Remove(dstTempPath)
	}()

	bufferPtr := uploadHashBufferPool.Get().(*[]byte)
	defer uploadHashBufferPool.Put(bufferPtr)
	// digest 在跨设备复制时同步接收源文件内容，避免复制完成后再打开文件计算摘要。
	digest := sha256.New()
	// hashingReader 把跨设备复制输入同时写入目标临时文件和 SHA-256 计算器。
	hashingReader := io.TeeReader(srcFile, digest)
	if _, err := io.CopyBuffer(dstFile, hashingReader, *bufferPtr); err != nil {
		_ = dstFile.Close()
		return errors.Wrap(err, "复制跨设备上传文件失败")
	}
	if actualHash := hex.EncodeToString(digest.Sum(nil)); expectedHash != "" && actualHash != expectedHash {
		_ = dstFile.Close()
		return errors.Errorf("上传文件完整性校验失败")
	}
	if err := dstFile.Chmod(0o644); err != nil {
		_ = dstFile.Close()
		return errors.Wrap(err, "设置跨设备上传文件权限失败")
	}
	if err := dstFile.Sync(); err != nil {
		_ = dstFile.Close()
		return errors.Wrap(err, "同步跨设备上传目标临时文件失败")
	}
	if err := dstFile.Close(); err != nil {
		return errors.Wrap(err, "关闭跨设备上传目标临时文件失败")
	}
	if err := os.Rename(dstTempPath, dstPath); err != nil {
		return errors.Wrap(err, "提交跨设备上传目标文件失败")
	}
	_ = os.Remove(srcPath)
	return nil
}

// pathsOnSameDevice 判断两个路径是否位于同一个设备号。
// 返回 ok=false 表示无法可靠判断，此时调用方应走原有校验后 rename 流程，避免误判导致安全边界变化。
func pathsOnSameDevice(leftPath string, rightPath string) (same bool, ok bool) {
	leftInfo, err := os.Stat(strings.TrimSpace(leftPath))
	if err != nil {
		return false, false
	}
	rightInfo, err := os.Stat(strings.TrimSpace(rightPath))
	if err != nil {
		return false, false
	}
	leftStat, leftOK := leftInfo.Sys().(*syscall.Stat_t)
	rightStat, rightOK := rightInfo.Sys().(*syscall.Stat_t)
	if !leftOK || !rightOK {
		return false, false
	}
	return leftStat.Dev == rightStat.Dev, true
}

// fileExists 判断本地路径是否存在。
func fileExists(filePath string) bool {
	if strings.TrimSpace(filePath) == "" {
		return false
	}
	_, err := os.Stat(filePath)
	return err == nil
}

// formatDateTime 使用项目统一格式输出时间字符串。
func formatDateTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format("2006-01-02 15:04:05")
}
