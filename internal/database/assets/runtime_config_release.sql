-- 用途：运行配置发布快照表。
-- 范围：仅 task_periodic 与 archive_jobs；基础设施配置和 workflows.user_tag 仍由 YAML 管理。

CREATE TABLE IF NOT EXISTS `runtime_config_release` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `version_no` bigint unsigned NOT NULL COMMENT '发布版本号',
  `snapshot_json` json NOT NULL COMMENT '发布快照JSON',
  `snapshot_yaml` mediumtext NOT NULL COMMENT '发布快照YAML',
  `checksum` char(64) NOT NULL COMMENT '快照SHA256',
  `base_release_id` bigint unsigned NOT NULL DEFAULT '0' COMMENT '来源发布ID',
  `restart_required` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否需要重启',
  `restart_reason` varchar(500) NOT NULL DEFAULT '' COMMENT '重启原因',
  `remark` varchar(500) NOT NULL DEFAULT '' COMMENT '发布备注',
  `published_by_admin_id` int unsigned NOT NULL DEFAULT '0' COMMENT '发布管理员ID',
  `published_by_name` varchar(64) NOT NULL DEFAULT '' COMMENT '发布管理员账号',
  `published_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '发布时间',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_version_no` (`version_no`),
  KEY `idx_published` (`published_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='运行配置发布快照';

-- 发布快照不写默认种子；DB 模式首次启动只发布初始化 SQL 写入的草稿表，草稿为空时拒绝启动。
