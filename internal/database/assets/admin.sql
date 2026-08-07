CREATE TABLE IF NOT EXISTS `admin` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT '主键',
  `name` varchar(20) NOT NULL DEFAULT '' COMMENT '用户账号',
  `real_name` varchar(20) NOT NULL DEFAULT '' COMMENT '用户名',
  `password` varchar(255) NOT NULL DEFAULT '' COMMENT '密码hash',
  `need_reset_password` tinyint unsigned NOT NULL DEFAULT '0' COMMENT '是否必须修改登录密码：0 否，1 是',
  `email` varchar(100) NOT NULL DEFAULT '' COMMENT '邮箱',
  `phone` varchar(30) NOT NULL DEFAULT '' COMMENT '电话',
  `mfa_secure_key` varchar(255) NOT NULL DEFAULT '' COMMENT '基于时间的动态密码 (TOTP) 多重身份验证 (MFA) 秘钥：如Google Authenticator、Microsoft Authenticator',
  `mfa_status` tinyint unsigned NOT NULL DEFAULT '0' COMMENT '启用 TOTP MFA (两步验证 2FA)：0 不启用，1 启用',
  `status` tinyint NOT NULL DEFAULT '1' COMMENT '账户状态: 1正常, 0禁用',
  `avatar` varchar(255) NOT NULL DEFAULT '' COMMENT '头像',
  `description` varchar(255) NOT NULL DEFAULT '' COMMENT '简介描述',
  `last_login_time` datetime NULL DEFAULT NULL COMMENT '最后登录时间，NULL 表示从未登录',
  `last_login_ip` varchar(45) NOT NULL DEFAULT '' COMMENT '最后登录 IP',
  `last_login_ipaddr` varchar(255) NOT NULL DEFAULT '' COMMENT '最后登录 IP 归属地',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '添加时间',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_name` (`name`),
  KEY `idx_avatar` (`avatar`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='管理员';

INSERT IGNORE INTO `admin` (`id`, `name`, `real_name`, `password`, `need_reset_password`, `email`, `phone`, `mfa_secure_key`, `mfa_status`, `status`, `avatar`, `description`, `last_login_time`, `last_login_ip`, `last_login_ipaddr`, `created_at`, `updated_at`) VALUES (1, 'super999', 'super999', '$2y$10$ory3FZfUy1VExaUHmEkeluYtVtP/4CiCCfeSPfD12T9dbpWqO52Eq', 1, '', '', '', 0, 1, '', '超级管理员', '2026-05-06 02:17:05', '127.0.0.1', '', '2022-03-21 21:54:26', '2026-05-06 15:46:07');
