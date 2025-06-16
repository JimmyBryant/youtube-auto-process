"""
视频处理主程序包入口

导出核心组件和版本信息
"""

__version__ = "1.0.0"
__all__ = ["TaskManager", "VideoProcessor", "TaskScheduler"]

import logging
from pathlib import Path
from .core.task_manager import TaskManager
from .core.models import TaskModel
from .modules import (
    VideoDownloader,
    AudioTranscriber,
    SubtitleTranslator,
    VideoPublisher
)

# 初始化包级日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler(Path(__file__).parent.parent / 'logs' / 'app.log'),
        logging.StreamHandler()
    ]
)

def get_version() -> str:
    """获取当前包版本"""
    return __version__