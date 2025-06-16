class CoreException(Exception):
    """基础异常类型"""
    def __init__(self, message: str, code: int = 400):
        super().__init__(message)
        self.code = code

class TaskNotFoundError(CoreException):
    """任务不存在异常"""
    def __init__(self, message: str = "Task not found"):
        super().__init__(message, code=404)

class TaskStateError(CoreException):
    """任务状态异常"""
    def __init__(self, message: str = "Invalid task state"):
        super().__init__(message, code=409)

class DatabaseError(CoreException):
    """数据库操作异常"""
    def __init__(self, message: str = "Database operation failed"):
        super().__init__(message, code=503)

class ConfigurationError(CoreException):
    """配置错误异常"""
    def __init__(self, message: str = "Invalid configuration"):
        super().__init__(message, code=500)