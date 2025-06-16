"""
核心模块初始化文件

定义包级共享对象、控制模块导出内容
"""

# 包版本信息
__version__ = "1.0.0"

# 控制导出范围
__all__ = [
    "TaskManager",
    "TaskModel",
    "TaskStatus",
    "TaskProgress",
    "TaskNotFoundError",
    "TaskStateError"
]

# 初始化日志配置
import logging
from .task_manager import TaskManager
from .models import TaskModel, TaskStatus, TaskProgress
from .exceptions import (
    TaskNotFoundError,
    TaskStateError,
    DatabaseError,
    ConfigurationError
)

# 包级共享日志器
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

def _check_dependencies():
    """验证必要依赖"""
    try:
        import pymongo
        from pydantic import BaseModel
    except ImportError as e:
        raise RuntimeError("缺少必要依赖包") from e

# 初始化时自动执行依赖检查
_check_dependencies()

# 包初始化完成提示
logger.info(f"Core module initialized (version {__version__})")