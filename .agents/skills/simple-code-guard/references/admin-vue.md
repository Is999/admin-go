# Admin Vue 项目边界

## 真实链路

- 可见功能沿 `views`、`router`、`api`、`store`、`components` 和 `locales` 闭环。
- 优先复用现有 Vben 组件、schema、hooks 和请求客户端，不建立平行 UI、表单或状态框架。
- 前端只承载交互和展示状态；权限、安全与数据规则由后端强制执行，前端限制不能成为唯一保护。

## 简化重点

- 单页面状态留在页面；没有稳定复用时不提取 composable、store 或通用组件。
- API wrapper 直接映射后端契约，不叠加只转发参数或改名的多层 wrapper。
- 表格、表单、弹窗和 action 使用现有 schema 与组件能力，不为少量字段创建配置生成器。
- 避免无边界 deep watch、重复请求、全量前端过滤和大数组复制；分页、取消、loading 与错误状态保持可见。
- 删除无路由、无菜单、无调用方的旧页面、类型、i18n key 和兼容字段。
- 性能优化优先减少网络请求、重复渲染和大对象响应式开销，不用难读缓存技巧优化普通页面。

## 不可破坏边界

- Route guard、菜单/按钮权限、MFA、签名、加密、session 和重复点击保护。
- 中英文 i18n 与后端业务码消息。
- loading、empty、error、disabled 和 pagination 的用户可见语义。

## 验证

- `pnpm -F @vben/web-antd run typecheck`
- 提交就绪时运行 `pnpm exec lefthook run pre-commit`
- TypeScript 源码或规则变化时运行受影响 package 的 lint；依赖、构建配置或产物边界变化时运行 build；页面、路由、表单、弹窗、权限或可见交互变化时运行浏览器冒烟
- `git diff --check`、`git diff --cached --check`、`git status --short`
