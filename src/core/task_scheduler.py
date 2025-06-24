import asyncio
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import uuid

from .task_manager import TaskManager
from .models import TaskStatus, TaskStage, StageStatus, TaskModel, ProcessingType
from src.modules.video_downloader import download_video
from src.modules.subtitle_generator import generate_subtitles
from src.modules.translation_service import translate_subtitles
from src.modules.comment_processor import fetch_comments, process_comments
from src.modules.video_editor import edit_video
from src.modules.publisher import VideoPublisher  # 修改为导入类

logger = logging.getLogger('task_scheduler')

class TaskScheduler:
    """增强版任务调度器，支持断点续做和详细阶段管理"""
    
    def __init__(self, 
                 max_concurrent_tasks: int = 3,
                 temp_base_dir: str = "/tmp/video_processing"):
        try:
            self.task_manager = TaskManager()
            self.max_concurrent_tasks = max_concurrent_tasks
            self.active_tasks: Dict[str, asyncio.Task] = {}
            self._stop_event = asyncio.Event()
            self.temp_base_dir = Path(temp_base_dir)
            self.temp_base_dir.mkdir(parents=True, exist_ok=True)
            
            # 阶段处理器映射
            self.stage_handlers = {
                TaskStage.DOWNLOADING: self._handle_downloading,
                # TaskStage.TRANSCRIBING: self._handle_transcribing,
                # TaskStage.TRANSLATING: self._handle_translating,
                # TaskStage.COMMENT_FETCHING: self._handle_comment_fetching,
                # TaskStage.COMMENT_PROCESSING: self._handle_comment_processing,
                # TaskStage.SYNTHESIZING: self._handle_synthesizing,
                # TaskStage.PUBLISHING: self._handle_publishing
            }
            
            # 阶段执行顺序
            self.stage_sequence = [
                TaskStage.DOWNLOADING,
                # TaskStage.TRANSCRIBING,
                # TaskStage.TRANSLATING,
                # TaskStage.COMMENT_FETCHING,
                # TaskStage.COMMENT_PROCESSING,
                # TaskStage.SYNTHESIZING,
                # TaskStage.PUBLISHING
            ]
            # 检查阶段处理器是否配置正确
            if not hasattr(self, 'stage_handlers'):
                raise RuntimeError("stage_handlers 未正确初始化")
            if not hasattr(self, 'stage_sequence'):
                raise RuntimeError("stage_sequence 未正确初始化")    
        except Exception as e:
            logger.error(f"调度器初始化失败: {str(e)}")
            raise
    async def start(self):
        """启动任务调度器主循环"""
        logger.info("🚀 Starting task scheduler main loop")
        while not self._stop_event.is_set():
            try:
                # 获取待处理任务
                pending_tasks = await self.task_manager.list_tasks(status=TaskStatus.PENDING)
                
                # 如果当前活跃任务数未达上限且有等待的任务
                if len(self.active_tasks) < self.max_concurrent_tasks and pending_tasks:
                    # 按优先级排序
                    pending_tasks.sort(key=lambda t: t.priority, reverse=True)
                    
                    # 启动新任务
                    for task in pending_tasks[:self.max_concurrent_tasks - len(self.active_tasks)]:
                        task_id = task.id
                        if task_id not in self.active_tasks:
                            logger.info(f"▶️ Starting processing for task {task_id}")
                            self.active_tasks[task_id] = asyncio.create_task(self._process_task(task))
                            self.active_tasks[task_id].add_done_callback(
                                lambda t, tid=task_id: self._task_done_callback(t, tid)
                            )
                
                # 等待一小段时间再检查
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"⚠️ Scheduler loop error: {str(e)}")
                await asyncio.sleep(5)  # 出错后等待更长时间
    async def _process_task(self, task: TaskModel):
        task_dir = None
        """处理单个任务"""
        try:
            # 创建唯一临时目录
            task_dir = self.temp_base_dir / f"task_{task.id}_{uuid.uuid4().hex[:6]}"
            task_dir.mkdir(exist_ok=True)
            # 更新任务状态为处理中
            await self.task_manager.update_task_status(task.id, TaskStatus.PROCESSING)
            
            # 按阶段顺序处理任务
            for stage in self.stage_sequence:
                success, outputs = await self.stage_handlers[stage](task, task_dir)
                if not success:
                    await self.task_manager.update_task_status(task.id, TaskStatus.FAILED)
                    return
                
                # 更新阶段状态
                await self.task_manager.update_stage_status(
                    task_id=task.id,
                    stage=stage,
                    status=StageStatus.COMPLETED,
                    output_files=outputs
                )
            
            # 所有阶段完成
            await self.task_manager.update_task_status(task.id, TaskStatus.COMPLETED)
            logger.info(f"🎉 Task {task.id} completed successfully")
            
        except Exception as e:
            logger.error(f"❌ Task {task.id} failed: {str(e)}")
            await self.task_manager.update_task_status(task.id, TaskStatus.FAILED, error=str(e))
        finally:
            # 仅清理已完成/失败的任务
            if task_dir and task.status in {TaskStatus.COMPLETED, TaskStatus.FAILED}:
                await self._cleanup_task_files(task_dir)

    async def monitor_status(self):
        """监控任务状态"""
        logger.info("📊 Starting task status monitor")
        while not self._stop_event.is_set():
            try:
                # 这里可以添加状态监控逻辑
                # 例如：记录活跃任务数、检查长时间运行的任务等
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"⚠️ Status monitor error: {str(e)}")
                await asyncio.sleep(30) 
    async def _handle_downloading(self, task: TaskModel, task_dir: str) -> Tuple[bool, Dict[str, str]]:
        """处理视频下载阶段"""
        logger.info(f"📥📥 Handling downloading for task {task.id}")
        
        stage_progress = task.stage_progress.get(TaskStage.DOWNLOADING)
        if stage_progress is not None and stage_progress.status == StageStatus.COMPLETED:
            logger.info(f"⏩⏩⏩ Skipping already completed downloading for task {task.id}")
            return True, stage_progress.output_files
    
        
        try:
            # 开始阶段
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.DOWNLOADING,
                status=StageStatus.PROCESSING
            )
            
            # 下载视频和封面
            video_path, thumbnail_path = await download_video(task.video_url, task_dir)
            
            outputs = {
                "video_path": str(video_path),
                "thumbnail_path": str(thumbnail_path)
            }
            
            return True, outputs
        except Exception as e:
            logger.error(f"❌❌ Download failed for task {task.id}: {str(e)}")
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.DOWNLOADING,
                status=StageStatus.FAILED,
                error=str(e)
            )
            return False, {}

    async def _handle_transcribing(self, task: TaskModel) -> Tuple[bool, Dict[str, str]]:
        """处理字幕生成阶段"""
        logger.info(f"🔤🔤 Handling transcribing for task {task.id}")
        
        stage_progress = task.stage_progress.get(TaskStage.TRANSCRIBING)
        if stage_progress.status == StageStatus.COMPLETED:
            logger.info(f"⏩⏩⏩ Skipping already completed transcribing for task {task.id}")
            return True, stage_progress.output_files
        
        # 检查前置阶段是否完成
        download_progress = task.stage_progress.get(TaskStage.DOWNLOADING)
        if download_progress.status != StageStatus.COMPLETED:
            raise RuntimeError(f"Download not completed for task {task.id}, can't transcribe")
        
        try:
            # 开始阶段
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.TRANSCRIBING,
                status=StageStatus.PROCESSING
            )
            
            # 获取视频路径
            video_path = Path(download_progress.output_files["video_path"])
            
            # 生成字幕
            task_dir = Path(task.temp_dir)
            subtitle_path = await generate_subtitles(video_path, task_dir)
            
            # 返回输出文件信息
            outputs = {
                "subtitle_path": str(subtitle_path)
            }
            
            return True, outputs
        except Exception as e:
            logger.error(f"❌❌ Transcribing failed for task {task.id}: {str(e)}")
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.TRANSCRIBING,
                status=StageStatus.FAILED,
                error=str(e)
            )
            return False, {}

    async def _handle_translating(self, task: TaskModel) -> Tuple[bool, Dict[str, str]]:
        """处理字幕翻译阶段"""
        logger.info(f"🌐🌐 Handling translating for task {task.id}")
        
        stage_progress = task.stage_progress.get(TaskStage.TRANSLATING)
        if stage_progress.status == StageStatus.COMPLETED:
            logger.info(f"⏩⏩⏩ Skipping already completed translating for task {task.id}")
            return True, stage_progress.output_files
        
        # 检查前置阶段是否完成
        transcribe_progress = task.stage_progress.get(TaskStage.TRANSCRIBING)
        if transcribe_progress.status != StageStatus.COMPLETED:
            raise RuntimeError(f"Transcribing not completed for task {task.id}, can't translate")
        
        try:
            # 开始阶段
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.TRANSLATING,
                status=StageStatus.PROCESSING
            )
            
            # 获取字幕路径
            subtitle_path = Path(transcribe_progress.output_files["subtitle_path"])
            
            # 翻译字幕
            task_dir = Path(task.temp_dir)
            translated_subtitle_path = await translate_subtitles(subtitle_path, task_dir)
            
            # 返回输出文件信息
            outputs = {
                "translated_subtitle_path": str(translated_subtitle_path)
            }
            
            return True, outputs
        except Exception as e:
            logger.error(f"❌❌ Translating failed for task {task.id}: {str(e)}")
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.TRANSLATING,
                status=StageStatus.FAILED,
                error=str(e)
            )
            return False, {}

    async def _handle_comment_fetching(self, task: TaskModel) -> Tuple[bool, Dict[str, str]]:
        """处理评论获取阶段"""
        logger.info(f"💬💬 Handling comment fetching for task {task.id}")
        
        stage_progress = task.stage_progress.get(TaskStage.COMMENT_FETCHING)
        if stage_progress.status == StageStatus.COMPLETED:
            logger.info(f"⏩⏩⏩ Skipping already completed comment fetching for task {task.id}")
            return True, stage_progress.output_files
        
        try:
            # 开始阶段
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.COMMENT_FETCHING,
                status=StageStatus.PROCESSING
            )
            
            # 获取评论
            task_dir = Path(task.temp_dir)
            comments_file = await fetch_comments(task.video_url, task_dir)
            
            # 返回输出文件信息
            outputs = {
                "comments_file": str(comments_file)
            }
            
            return True, outputs
        except Exception as e:
            logger.error(f"❌❌ Comment fetching failed for task {task.id}: {str(e)}")
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.COMMENT_FETCHING,
                status=StageStatus.FAILED,
                error=str(e)
            )
            return False, {}

    async def _handle_comment_processing(self, task: TaskModel) -> Tuple[bool, Dict[str, str]]:
        """处理评论处理阶段"""
        logger.info(f"🖼🖼 Handling comment processing for task {task.id}")
        
        stage_progress = task.stage_progress.get(TaskStage.COMMENT_PROCESSING)
        if stage_progress.status == StageStatus.COMPLETED:
            logger.info(f"⏩⏩⏩ Skipping already completed comment processing for task {task.id}")
            return True, stage_progress.output_files
        
        # 检查前置阶段是否完成
        fetch_progress = task.stage_progress.get(TaskStage.COMMENT_FETCHING)
        if fetch_progress.status != StageStatus.COMPLETED:
            raise RuntimeError(f"Comment fetching not completed for task {task.id}, can't process")
        
        try:
            # 开始阶段
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.COMMENT_PROCESSING,
                status=StageStatus.PROCESSING
            )
            
            # 获取评论文件路径
            comments_file = Path(fetch_progress.output_files["comments_file"])
            
            # 处理评论并生成图片
            task_dir = Path(task.temp_dir)
            comment_images = await process_comments(comments_file, task_dir)
            
            # 返回输出文件信息
            outputs = {
                "comment_images": [str(img) for img in comment_images]
            }
            
            return True, outputs
        except Exception as e:
            logger.error(f"❌❌ Comment processing failed for task {task.id}: {str(e)}")
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.COMMENT_PROCESSING,
                status=StageStatus.FAILED,
                error=str(e)
            )
            return False, {}

    async def _handle_synthesizing(self, task: TaskModel) -> Tuple[bool, Dict[str, str]]:
        """处理视频合成阶段"""
        logger.info(f"🎬🎬 Handling synthesizing for task {task.id}")
        
        stage_progress = task.stage_progress.get(TaskStage.SYNTHESIZING)
        if stage_progress.status == StageStatus.COMPLETED:
            logger.info(f"⏩⏩⏩ Skipping already completed synthesizing for task {task.id}")
            return True, stage_progress.output_files
        
        # 检查前置阶段是否完成
        required_stages = [
            (TaskStage.DOWNLOADING, "Downloading"),
            (TaskStage.TRANSLATING, "Translating"),
            (TaskStage.COMMENT_PROCESSING, "Comment processing")
        ]
        
        for stage, name in required_stages:
            progress = task.stage_progress.get(stage)
            if progress.status != StageStatus.COMPLETED:
                raise RuntimeError(f"{name} not completed for task {task.id}, can't synthesize")
        
        try:
            # 开始阶段
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.SYNTHESIZING,
                status=StageStatus.PROCESSING
            )
            
            # 获取所需文件路径
            download_progress = task.stage_progress.get(TaskStage.DOWNLOADING)
            translate_progress = task.stage_progress.get(TaskStage.TRANSLATING)
            comment_progress = task.stage_progress.get(TaskStage.COMMENT_PROCESSING)
            
            video_path = Path(download_progress.output_files["video_path"])
            translated_subtitle_path = Path(translate_progress.output_files["translated_subtitle_path"])
            comment_images = [Path(img) for img in comment_progress.output_files["comment_images"]]
            
            # 合成视频
            task_dir = Path(task.temp_dir)
            output_path = await edit_video(
                video_path=video_path,
                subtitle_path=translated_subtitle_path,
                comment_images=comment_images,
                output_dir=task_dir
            )
            
            # 更新任务输出文件路径
            await self.task_manager.save_task(task)
            
            # 返回输出文件信息
            outputs = {
                "output_path": str(output_path)
            }
            
            return True, outputs
        except Exception as e:
            logger.error(f"❌❌ Synthesizing failed for task {task.id}: {str(e)}")
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.SYNTHESIZING,
                status=StageStatus.FAILED,
                error=str(e)
            )
            return False, {}

    async def _handle_publishing(self, task: TaskModel) -> Tuple[bool, Dict[str, str]]:
        """处理视频发布阶段"""
        logger.info(f"📤📤 Handling publishing for task {task.id}")
        
        stage_progress = task.stage_progress.get(TaskStage.PUBLISHING)
        if stage_progress.status == StageStatus.COMPLETED:
            logger.info(f"⏩⏩⏩ Skipping already completed publishing for task {task.id}")
            return True, stage_progress.output_files
        
        # 检查前置阶段是否完成
        synthesize_progress = task.stage_progress.get(TaskStage.SYNTHESIZING)
        if synthesize_progress.status != StageStatus.COMPLETED:
            raise RuntimeError(f"Synthesizing not completed for task {task.id}, can't publish")
        
        try:
            # 开始阶段
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.PUBLISHING,
                status=StageStatus.PROCESSING
            )
            
            # 获取输出文件路径
            output_path = Path(synthesize_progress.output_files["output_path"])
            
            # 发布视频
            publish_result = await publish_video(output_path, task.metadata)
            
            # 返回发布结果
            outputs = {
                "publish_result": publish_result
            }
            
            return True, outputs
        except Exception as e:
            logger.error(f"❌❌ Publishing failed for task {task.id}: {str(e)}")
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.PUBLISHING,
                status=StageStatus.FAILED,
                error=str(e)
            )
            return False, {}

    # ... (保留之前的 _task_done_callback, _cleanup, _cleanup_task_files 方法)
            
    def _task_done_callback(self, task: asyncio.Task, task_id: str) -> None:
        """任务完成回调"""
        self.active_tasks.pop(task_id, None)
        if task.exception():
            logger.error(f"❌❌ Task {task_id} raised an exception", exc_info=task.exception())
            
    async def _cleanup(self) -> None:
        """清理资源"""
        logger.info("🧹🧹 Cleaning up resources...")
        for task_id, task in self.active_tasks.items():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logger.debug(f"Task {task_id} cancelled")
                    
        self.active_tasks.clear()
        logger.info("✅ Task scheduler cleanup completed")
        
    async def _cleanup_task_files(self, task_dir: Path):
        """清理任务临时文件"""
        try:
            if task_dir.exists() and task_dir.is_dir():
                shutil.rmtree(task_dir)
                logger.debug(f"🧹🧹 Cleaned up files for task: {task_dir.name}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to clean up task files: {str(e)}")