from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Literal, Any
from bson import ObjectId
from pydantic import BaseModel, Field, ConfigDict, computed_field, field_validator

class TaskStatus(str, Enum):
    """任务主状态"""
    PENDING = "pending"          # 等待处理
    PROCESSING = "processing"    # 处理中（有子状态）
    PAUSED = "paused"            # 已暂停
    COMPLETED = "completed"      # 已完成
    FAILED = "failed"            # 已失败
    CANCELLED = "cancelled"      # 已取消

class TaskStage(str, Enum):
    """视频处理全流程子状态"""
    DOWNLOADING = "downloading"              # 视频下载中
    COMMENT_FETCHING = "comment_fetching"    # 获取视频评论
    AUDIO_EXTRACTING = "audio_extracting"    # 提取音频流
    AUDIO_ENHANCING = "audio_enhancing"      # 音频降噪/增强
    TRANSCRIBING = "transcribing"            # 语音转文字
    TRANSLATING = "translating"              # 文本翻译
    SUBTITLE_GENERATING = "subtitle_generating" # 字幕生成
    VIDEO_ANALYZING = "video_analyzing"      # 视频内容分析
    EFFECT_ADDING = "effect_adding"          # 特效/滤镜添加
    CLIP_EDITING = "clip_editing"            # 片段剪辑
    SYNTHESIZING = "synthesizing"            # 最终合成渲染
    QUALITY_CHECK = "quality_check"          # 成品质量检测
    PUBLISHING = "publishing"                # 平台发布
    MANUAL_REVIEW = "manual_review"          # 人工审核

class ProcessingType(str, Enum):
    """视频分析的具体类型"""
    OBJECT_DETECTION = "object_detection"    # 物体识别
    SCENE_CLASSIFICATION = "scene_class"    # 场景分类
    EMOTION_ANALYSIS = "emotion_analysis"    # 情绪分析
    KEYFRAME_EXTRACTION = "keyframe_extract" # 关键帧提取

class TaskProgress(BaseModel):
    """增强版进度跟踪"""
    current: int = Field(..., ge=0, description="当前进度值")
    total: int = Field(..., gt=0, description="总进度值")
    speed: Optional[float] = Field(None, description="处理速度（单位/秒）")
    remaining: Optional[float] = Field(None, description="预计剩余时间（秒）")
    message: Optional[str] = Field(None, description="当前状态描述")
    extra: Optional[Dict[str, Any]] = Field(None, description="扩展数据")

# 预生成所有时间戳键
_TIMESTAMP_KEYS = [
    "created_at", "started_at", "completed_at",
    *(f"{stage.value}_start" for stage in TaskStage),
    *(f"{stage.value}_end" for stage in TaskStage)
]
TimestampKey = Literal[tuple(_TIMESTAMP_KEYS)]  # type: ignore

class TaskModel(BaseModel):
    """完整的任务数据模型"""
    id: Optional[str] = Field(
        default=None,
        alias="_id",
        description="任务ID（MongoDB ObjectId的字符串形式）"
    )
    
    @field_validator("id", mode="before")
    def convert_objectid(cls, v):
        if v is None:
            return None
        if isinstance(v, ObjectId):
            return str(v)
        if isinstance(v, str) and ObjectId.is_valid(v):
            return v
        raise ValueError("Invalid ID format")
    video_url: str = Field(..., description="视频源URL")
    status: TaskStatus = Field(
        default=TaskStatus.PENDING,
        description="任务主状态"
    )
    stage: Optional[TaskStage] = Field(
        None,
        description="当前处理子状态"
    )
    processing_type: Optional[ProcessingType] = Field(
        None,
        description="视频分析时的具体类型"
    )
    progress: Dict[TaskStage, Optional[TaskProgress]] = Field(
        default_factory=dict,
        description="各阶段进度记录"
    )
    priority: int = Field(
        default=5,
        ge=1, le=9,
        description="任务优先级(1-9)"
    )
    timestamps: Dict[TimestampKey, Optional[datetime]] = Field(
        default_factory=lambda: dict.fromkeys(_TIMESTAMP_KEYS, None) | {
            "created_at": datetime.now(timezone.utc)
        },
        description="各阶段时间记录"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="任务元数据"
    )
    error: Optional[str] = Field(
        None,
        description="错误信息"
    )

    def start_stage(
        self,
        stage: TaskStage,
        processing_type: Optional[ProcessingType] = None
    ):
        """开始处理阶段（带类型校验）"""
        if stage == TaskStage.VIDEO_ANALYZING and not processing_type:
            raise ValueError("视频分析阶段必须指定processing_type")
        
        self.status = TaskStatus.PROCESSING
        self.stage = stage
        self.processing_type = processing_type
        self.timestamps[f"{stage.value}_start"] = datetime.now(timezone.utc)
        
        if not self.timestamps["started_at"]:
            self.timestamps["started_at"] = datetime.now(timezone.utc)

    def end_stage(self):
        """结束当前阶段"""
        if not self.stage:
            raise ValueError("没有正在进行的阶段")
        
        self.timestamps[f"{self.stage.value}_end"] = datetime.now(timezone.utc)
        self.stage = None
        self.processing_type = None

    def set_failed(self, error_msg: str):
        """标记任务失败"""
        self.status = TaskStatus.FAILED
        self.error = error_msg
        self.timestamps["completed_at"] = datetime.now(timezone.utc)
        if self.stage:
            self.end_stage()

    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat(),
            ObjectId: lambda v: str(v)
        },
        use_enum_values=True,
        arbitrary_types_allowed=True,
        json_schema_extra={
            "example": {
                "video_url": "https://youtu.be/example",
                "status": "pending",
                "metadata": {
                    "resolution": "1080p",
                    "duration": 120
                }
            }
        }
    )

# 辅助类型（与TimestampKey保持一致）
TaskTimestamps = Dict[TimestampKey, Optional[datetime]]