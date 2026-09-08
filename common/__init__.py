"""
FlexiChrono 公共库
5个后端服务共享的工具模块，消除重复代码。

模块说明：
- step_timer: 耗时统计工具（原各服务各自实现的 StepTimer）
- service_client: 统一服务间 HTTP 客户端（带熔断、重试、健康检查）
- logger: 统一日志配置
- security: 密码哈希、token 验证等安全工具
- config: 统一配置加载（.env 支持、环境变量校验）
"""
__version__ = "1.0.0"
