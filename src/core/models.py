from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Literal, Any, List
from bson import ObjectId
from pydantic import BaseModel, Field, ConfigDict, computed_field, field_validator
from pathlib import Path

class TaskStatus(str, Enum):
    """任务主状态枚举"""
    PENDING = "pending"
    PROCESSING = "processing"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class StageStatus(str, Enum):
    """阶段状态枚举"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class TaskStage(str, Enum):
    """任务阶段枚举"""
    DOWNLOADING = "downloading"
    TRANSCRIBING = "transcribing"
    TRANSLATING = "translating"
    SUBTITLE_SPLITTING = "subtitle_splitting"
    COMMENT_FETCHING = "comment_fetching"
    COMMENT_PROCESSING = "comment_processing"
    SYNTHESIZING = "synthesizing"
    PUBLISHING = "publishing"

class ProcessingType(str, Enum):
    """处理类型枚举"""
    OBJECT_DETECTION = "object_detection"
    SCENE_CLASSIFICATION = "scene_classification"
    EMOTION_ANALYSIS = "emotion_analysis"

class TaskProgress(BaseModel):
    """任务进度详情"""
    current: int = Field(..., ge=0)
    total: int = Field(..., gt=0)
    speed: Optional[float] = None
    remaining: Optional[float] = None
    message: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None

class StageProgress(BaseModel):
    """阶段进度详情（包含输出文件路径）"""
    status: StageStatus = Field(default=StageStatus.PENDING)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress: Optional[TaskProgress] = None
    message: Optional[str] = None
    error: Optional[str] = None
    output_files: Dict[str, str] = Field(default_factory=dict)  # 关键修改：存储文件路径
    output_data: Optional[Any] = None

# 预生成时间戳键
_TIMESTAMP_KEYS = [
    "created_at", "started_at", "completed_at",
    *(f"{stage.value}_start" for stage in TaskStage),
    *(f"{stage.value}_end" for stage in TaskStage)
]
TimestampKey = Literal[
    "created_at",
    "started_at",
    "completed_at",
    "downloading_start",
    "transcribing_start",
    "translating_start",
    "subtitle_splitting_start",
    "comment_fetching_start",
    "comment_processing_start",
    "synthesizing_start",
    "publishing_start",
    "downloading_end",
    "transcribing_end",
    "translating_end",
    "subtitle_splitting_end",
    "comment_fetching_end",
    "comment_processing_end",
    "synthesizing_end",
    "publishing_end"
]

class TaskModel(BaseModel):
    """完整的任务数据模型（优化版）"""
    
    # --- 核心字段 ---
    id: Optional[str] = Field(None, alias="_id")
    video_url: str = Field(...)
    status: TaskStatus = Field(default=TaskStatus.PENDING)
    stage: Optional[TaskStage] = None
    processing_type: Optional[ProcessingType] = None
    priority: int = Field(default=5, ge=1, le=9)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None
    manual_resume: bool = Field(default=False, description="3次失败后需手动恢复")
    
    # --- 阶段管理 ---
    stage_progress: Dict[TaskStage, StageProgress] = Field(default_factory=dict)
    timestamps: Dict[TimestampKey, Optional[datetime]] = Field(
        default_factory=lambda: dict.fromkeys(_TIMESTAMP_KEYS, None) | {
            "created_at": datetime.now(timezone.utc)
        }
    )

    # --- 计算方法 ---
    @computed_field
    @property
    def downloaded_video_path(self) -> Optional[str]:
        """获取下载的视频路径（从阶段数据中读取）"""
        return self.get_stage_file(TaskStage.DOWNLOADING, "video_path")

    @computed_field
    @property
    def downloaded_thumbnail_path(self) -> Optional[str]:
        """获取下载的封面路径（从阶段数据中读取）"""
        return self.get_stage_file(TaskStage.DOWNLOADING, "thumbnail_path")

    @computed_field
    @property
    def subtitle_path(self) -> Optional[str]:
        """获取生成的字幕路径"""
        return self.get_stage_file(TaskStage.TRANSCRIBING, "subtitle_path")

    # --- 核心方法 ---
    def get_stage_file(self, stage: TaskStage, file_key: str) -> Optional[str]:
        """统一获取阶段输出文件路径"""
        if stage in self.stage_progress:
            return self.stage_progress[stage].output_files.get(file_key)
        return None

    def start_stage(self, stage: TaskStage, processing_type: Optional[ProcessingType] = None):
        """开始处理阶段"""
        if stage == TaskStage.VIDEO_ANALYZING and not processing_type:
            raise ValueError("Video analysis requires processing_type")
            
        self.status = TaskStatus.PROCESSING
        self.stage = stage
        self.processing_type = processing_type
        
        # 初始化阶段进度
        if stage not in self.stage_progress:
            self.stage_progress[stage] = StageProgress()
        
        # 更新状态
        self.stage_progress[stage].status = StageStatus.PROCESSING
        self.stage_progress[stage].started_at = datetime.now(timezone.utc)
        
        # 更新时间戳
        self.timestamps[f"{stage.value}_start"] = datetime.now(timezone.utc)
        if not self.timestamps["started_at"]:
            self.timestamps["started_at"] = datetime.now(timezone.utc)

    def end_stage(self, output_files: Dict[str, str]):
        """成功完成阶段"""
        if not self.stage:
            raise ValueError("No active stage to end")
            
        self.stage_progress[self.stage].status = StageStatus.COMPLETED
        self.stage_progress[self.stage].completed_at = datetime.now(timezone.utc)
        self.stage_progress[self.stage].output_files = output_files  # 存储输出路径
        
        self.timestamps[f"{self.stage.value}_end"] = datetime.now(timezone.utc)
        self.stage = None
        self.processing_type = None

    def fail_stage(self, error: str):
        """标记阶段失败"""
        if not self.stage:
            raise ValueError("No active stage to fail")
            
        self.stage_progress[self.stage].status = StageStatus.FAILED
        self.stage_progress[self.stage].error = error
        self.stage_progress[self.stage].completed_at = datetime.now(timezone.utc)
        
        self.timestamps[f"{self.stage.value}_end"] = datetime.now(timezone.utc)
        self.stage = None
        self.processing_type = None

    def set_failed(self, error: str):
        """标记整个任务失败"""
        self.status = TaskStatus.FAILED
        self.error = error
        self.timestamps["completed_at"] = datetime.now(timezone.utc)
        if self.stage:
            self.fail_stage(error)

    # --- 配置 ---
    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat(),
            ObjectId: lambda v: str(v)
        },
        use_enum_values=True,
        arbitrary_types_allowed=True
    )

    @field_validator("id", mode="before")
    def convert_objectid(cls, v):
        """ID字段验证器"""
        if v is None:
            return None
        if isinstance(v, ObjectId):
            return str(v)
        if isinstance(v, str) and ObjectId.is_valid(v):
            return v
        raise ValueError("Invalid ID format")