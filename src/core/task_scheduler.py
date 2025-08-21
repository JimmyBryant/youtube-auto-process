
import asyncio
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import uuid
import json
import re
from .task_manager import TaskManager
from .models import TaskStatus, TaskStage, StageStatus, TaskModel, ProcessingType
from src.modules.video_downloader import download_video
from src.modules.transcriber import AudioTranscriber
import os
from src.modules.translation_service import TranslationService
from src.modules.subtitle_splitting import split_srt_file
from src.modules.comment_processor import fetch_comments, translate_comments


logger = logging.getLogger('task_scheduler')

class TaskScheduler:
    """增强版任务调度器，支持断点续做和详细阶段管理"""
    
    def __init__(self, 
                 max_concurrent_tasks: int = 3,
                 temp_base_dir: str = "/tmp/video_processing",
                 cookie_file: Path = None):
        try:
            self.task_manager = TaskManager()
            self.max_concurrent_tasks = max_concurrent_tasks
            self.active_tasks: Dict[str, asyncio.Task] = {}
            self._stop_event = asyncio.Event()
            self.temp_base_dir = Path(temp_base_dir)
            self.temp_base_dir.mkdir(parents=True, exist_ok=True)
            self.cookie_file = cookie_file
            # 阶段处理器映射
            self.stage_handlers = {
                TaskStage.DOWNLOADING: lambda task, task_dir: self._handle_downloading(task, task_dir, self.cookie_file),
                TaskStage.TRANSCRIBING: lambda task, task_dir: self._handle_transcribing(task),
                TaskStage.TRANSLATING: lambda task, task_dir: self._handle_translating(task),
                TaskStage.SUBTITLE_SPLITTING: lambda task, task_dir: self._handle_subtitle_splitting(task),
                TaskStage.COMMENT_FETCHING: lambda task, task_dir: self._handle_comment_fetching(task, task_dir),
                TaskStage.COMMENT_TRANSLATING: lambda task, task_dir: self._handle_comment_translating(task, task_dir),
                TaskStage.SYNTHESIZING: lambda task, task_dir: self._handle_synthesizing(task),
                # TaskStage.PUBLISHING: lambda task, task_dir: self._handle_publishing(task, task_dir),
            }
            
            # 阶段执行顺序
            self.stage_sequence = [
                TaskStage.DOWNLOADING,
                TaskStage.TRANSCRIBING,
                TaskStage.TRANSLATING,
                TaskStage.SUBTITLE_SPLITTING,
                TaskStage.COMMENT_FETCHING,
                TaskStage.COMMENT_TRANSLATING,
                TaskStage.SYNTHESIZING,
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
        """启动任务调度器主循环，支持 failed 任务断点续做，3次失败后需手动恢复，自动检测僵尸阶段"""
        logger.info("🚀 Starting task scheduler main loop")
        ZOMBIE_TIMEOUT = 30 * 60  # 30分钟未更新即视为僵尸任务，可根据需要调整
        while not self._stop_event.is_set():
            try:
                # 获取待处理和失败的任务
                tasks = await self.task_manager.list_tasks(limit=100)
                now = datetime.now(timezone.utc)
                # 自动检测并恢复僵尸阶段（processing状态长时间未更新）
                for task in tasks:
                    for stage, progress in (task.stage_progress or {}).items():
                        if getattr(progress, 'status', None) == 'processing':
                            updated_at = getattr(progress, 'updated_at', None)
                            if updated_at and (now - updated_at).total_seconds() > ZOMBIE_TIMEOUT:
                                logger.warning(f"检测到僵尸阶段: 任务{task.id} 阶段{stage}，自动重置为pending")
                                await self.task_manager.update_stage_status(
                                    task_id=task.id,
                                    stage=stage,
                                    status='pending',
                                    error='自动检测到僵尸阶段，已重置为pending'
                                )
                # 只自动处理未标记 manual_resume 的任务
                candidate_tasks = [t for t in tasks if t.status in [TaskStatus.PENDING, TaskStatus.FAILED] and not getattr(t, 'manual_resume', False)]
                if len(self.active_tasks) < self.max_concurrent_tasks and candidate_tasks:
                    candidate_tasks.sort(key=lambda t: t.priority, reverse=True)
                    for task in candidate_tasks[:self.max_concurrent_tasks - len(self.active_tasks)]:
                        task_id = task.id
                        if task_id not in self.active_tasks:
                            logger.info(f"▶️ Starting processing for task {task_id}")
                            self.active_tasks[task_id] = asyncio.create_task(self._process_task(task))
                            self.active_tasks[task_id].add_done_callback(
                                lambda t, tid=task_id: self._task_done_callback(t, tid)
                            )
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"⚠️ Scheduler loop error: {str(e)}")
                await asyncio.sleep(5)
    async def _process_task(self, task: TaskModel):
        """处理单个任务，支持断点续做，阶段失败自动重试3次，3次失败后需手动恢复"""
        task_dir = None
        try:
            # 1. 优先复用数据库中的 temp_dir
            if getattr(task, 'temp_dir', None):
                task_dir = Path(task.temp_dir)
            else:
                # 2. 没有则新建并写入数据库
                task_dir = self.temp_base_dir / f"task_{task.id}_{uuid.uuid4().hex[:6]}"
                task_dir.mkdir(exist_ok=True)
                # 写入数据库
                task.temp_dir = str(task_dir)
                # 立即保存到数据库，确保断点续做可用
                await self.task_manager.save_task(task)

            # 更新任务状态为处理中
            await self.task_manager.update_task_status(task.id, TaskStatus.PROCESSING)

            for stage in self.stage_sequence:
                # 每次循环都刷新最新 task 状态，确保依赖的阶段状态是最新的
                task = await self.task_manager.get_task_by_id(task.id)
                # 保证 task_dir 一致
                if getattr(task, 'temp_dir', None):
                    task_dir = Path(task.temp_dir)
                stage_progress = task.stage_progress.get(stage)
                if stage_progress and stage_progress.status == StageStatus.COMPLETED:
                    logger.info(f"⏩ 跳过已完成阶段 {stage} for task {task.id}")
                    continue
                max_retries = 3
                for attempt in range(1, max_retries + 1):
                    success, outputs = await self.stage_handlers[stage](task, task_dir)
                    if success:
                        await self.task_manager.update_stage_status(
                            task_id=task.id,
                            stage=stage,
                            status=StageStatus.COMPLETED,
                            output_files=outputs
                        )
                        break
                    else:
                        logger.warning(f"阶段 {stage} 第 {attempt} 次执行失败，任务ID: {task.id}")
                        if attempt < max_retries:
                            await asyncio.sleep(3)
                else:
                    logger.error(f"阶段 {stage} 连续3次失败，任务ID: {task.id}，任务终止")
                    # 标记任务为FAILED并加manual_resume
                    await self.task_manager.update_task_status(task.id, TaskStatus.FAILED, extra={"manual_resume": True})
                    return

            await self.task_manager.update_task_status(task.id, TaskStatus.COMPLETED)
            logger.info(f"🎉 Task {task.id} completed successfully")

        except Exception as e:
            logger.error(f"❌ Task {task.id} failed: {str(e)}")
            await self.task_manager.update_task_status(task.id, TaskStatus.FAILED, error=str(e), extra={"manual_resume": True})
        finally:
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
    async def _handle_downloading(self, task: TaskModel, task_dir: str, cookie_file: Path = None) -> Tuple[bool, Dict[str, str]]:
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
            # 优先使用传入的 cookie_file，其次环境变量和配置文件
            import os
            final_cookie_file = cookie_file
            if final_cookie_file is None:
                cookie_path = os.getenv('YT_COOKIE_PATH')
                if not cookie_path:
                    from dotenv import dotenv_values
                    config_path = Path(__file__).parent.parent.parent / 'config' / 'dev.env'
                    config = dotenv_values(config_path)
                    cookie_path = config.get('YT_COOKIE_PATH')
                final_cookie_file = Path(cookie_path) if cookie_path else None
            # 下载视频和封面
            video_path, thumbnail_path = await download_video(task.video_url, task_dir, final_cookie_file)
            
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
        """处理字幕生成阶段（whisperx medium）"""
        logger.info(f"🔤🔤 Handling transcribing for task {task.id}")
        stage_progress = task.stage_progress.get(TaskStage.TRANSCRIBING)
        if stage_progress is not None and stage_progress.status == StageStatus.COMPLETED:
            logger.info(f"⏩⏩⏩ Skipping already completed transcribing for task {task.id}")
            return True, stage_progress.output_files
        # 检查前置阶段是否完成
        download_progress = task.stage_progress.get(TaskStage.DOWNLOADING)
        if download_progress is None or download_progress.status != StageStatus.COMPLETED:
            raise RuntimeError(f"Download not completed for task {task.id}, can't transcribe")
        try:
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.TRANSCRIBING,
                status=StageStatus.PROCESSING
            )
            video_path = Path(download_progress.output_files["video_path"])
            transcriber = AudioTranscriber(model_size="medium", device="cpu")
            srt_path = await transcriber.transcribe(task.id, video_path)
            outputs = {"subtitle_path": str(srt_path)} if srt_path else {}
            return (srt_path is not None), outputs
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
        """处理字幕翻译阶段，支持多家大语言模型API，目标语言和API KEY通过环境变量配置，自动分段翻译"""
        logger.info(f"🌐🌐 Handling translating for task {task.id}")

        stage_progress = task.stage_progress.get(TaskStage.TRANSLATING)
        if stage_progress and stage_progress.status == StageStatus.COMPLETED:
            logger.info(f"⏩⏩⏩ Skipping already completed translating for task {task.id}")
            return True, stage_progress.output_files

        # 检查前置阶段是否完成
        transcribe_progress = task.stage_progress.get(TaskStage.TRANSCRIBING)
        if not transcribe_progress or transcribe_progress.status != StageStatus.COMPLETED:
            raise RuntimeError(f"Transcribing not completed for task {task.id}, can't translate")

        try:
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.TRANSLATING,
                status=StageStatus.PROCESSING
            )

            subtitle_path = Path(transcribe_progress.output_files["subtitle_path"])
            # 使用 TranslationService 类进行翻译
            translation_service = TranslationService()
            translated_srt_path = await translation_service.translate_subtitle(subtitle_path)
            outputs = {"translated_subtitle_path": str(translated_srt_path)}
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

    async def _handle_subtitle_splitting(self, task: TaskModel) -> Tuple[bool, Dict[str, str]]:
        """处理字幕分割阶段，确保原始和翻译后的 SRT 每行不超长，输出新 SRT 文件路径"""
        logger.info(f"✂️✂️ Handling subtitle splitting for task {task.id}")
        stage_progress = task.stage_progress.get(TaskStage.SUBTITLE_SPLITTING)
        if stage_progress and stage_progress.status == StageStatus.COMPLETED:
            logger.info(f"⏩⏩⏩ Skipping already completed subtitle splitting for task {task.id}")
            return True, stage_progress.output_files

        # 检查前置阶段
        transcribe_progress = task.stage_progress.get(TaskStage.TRANSCRIBING)
        translate_progress = task.stage_progress.get(TaskStage.TRANSLATING)
        if not transcribe_progress or transcribe_progress.status != StageStatus.COMPLETED:
            raise RuntimeError(f"Transcribing not completed for task {task.id}, can't split subtitles")
        if not translate_progress or translate_progress.status != StageStatus.COMPLETED:
            raise RuntimeError(f"Translating not completed for task {task.id}, can't split subtitles")

        try:
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.SUBTITLE_SPLITTING,
                status=StageStatus.PROCESSING
            )

            orig_srt_path = Path(transcribe_progress.output_files["subtitle_path"])
            trans_srt_path = Path(translate_progress.output_files["translated_subtitle_path"])

            # 输出新文件名
            orig_split_path = orig_srt_path.parent / (orig_srt_path.stem + ".split.srt")
            trans_split_path = trans_srt_path.parent / (trans_srt_path.stem + ".split.srt")

            # 分割原文和译文 SRT
            split_srt_file(str(orig_srt_path), str(orig_split_path), max_line_length=32)
            split_srt_file(str(trans_srt_path), str(trans_split_path), max_line_length=32)

            outputs = {
                "split_subtitle_path": str(orig_split_path),
                "split_translated_subtitle_path": str(trans_split_path)
            }
            return True, outputs
        except Exception as e:
            logger.error(f"❌❌ Subtitle splitting failed for task {task.id}: {str(e)}")
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.SUBTITLE_SPLITTING,
                status=StageStatus.FAILED,
                error=str(e)
            )
            return False, {}

    async def _handle_comment_fetching(self, task: TaskModel, task_dir: Path) -> Tuple[bool, Dict[str, str]]:
        """处理评论获取阶段"""
        logger.info(f"💬💬 Handling comment fetching for task {task.id}")
        
        stage_progress = task.stage_progress.get(TaskStage.COMMENT_FETCHING)
        if stage_progress is not None and stage_progress.status == StageStatus.COMPLETED:
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


    async def _handle_comment_translating(self, task: TaskModel, task_dir: Path) -> Tuple[bool, Dict[str, str]]:
        """处理评论翻译阶段，使用 comment_processor 模块的函数"""
        logger.info(f"🌍 Handling comment translating for task {task.id}")
        
        # 检查阶段是否已完成
        stage_progress = task.stage_progress.get(TaskStage.COMMENT_TRANSLATING)
        if stage_progress and stage_progress.status == StageStatus.COMPLETED:
            logger.info(f"⏩ Skipping already completed comment translating for task {task.id}")
            return True, stage_progress.output_files

        # 检查前置阶段是否完成
        fetch_progress = task.stage_progress.get(TaskStage.COMMENT_FETCHING)
        if not fetch_progress or fetch_progress.status != StageStatus.COMPLETED:
            raise RuntimeError(f"Comment fetching not completed for task {task.id}, can't translate comments")

        try:
            # 更新阶段状态为处理中
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.COMMENT_TRANSLATING,
                status=StageStatus.PROCESSING
            )
            
            # 使用 comment_processor 模块的 translate_comments 函数
            translated_count = await translate_comments(
                video_url=task.video_url,
                target_lang='zh'  # 默认翻译为中文
            )
            
            outputs = {
                "translated_count": translated_count,
                "video_url": task.video_url
            }
            
            logger.info(f"✅ Successfully translated {translated_count} comments for task {task.id}")
            return True, outputs
            
        except Exception as e:
            logger.error(f"❌ Comment translating failed for task {task.id}: {str(e)}")
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.COMMENT_TRANSLATING,
                status=StageStatus.FAILED,
                error=str(e)
            )
            return False, {}
    async def _handle_synthesizing(self, task: TaskModel) -> Tuple[bool, Dict[str, str]]:
        """处理视频合成阶段：将原字幕、中文字幕和带动画的评论插入视频（通过SyntheticVideo类）"""
        logger.info(f"🎬 Handling synthesizing for task {task.id}")
        stage_progress = task.stage_progress.get(TaskStage.SYNTHESIZING)
        if stage_progress and stage_progress.status == StageStatus.COMPLETED:
            logger.info(f"⏩ Skipping already completed synthesizing for task {task.id}")
            return True, stage_progress.output_files

        # 检查前置阶段是否完成
        required_stages = [
            (TaskStage.DOWNLOADING, "Downloading"),
            (TaskStage.TRANSLATING, "Translating"),
            (TaskStage.SUBTITLE_SPLITTING, "Subtitle splitting"),
            (TaskStage.COMMENT_TRANSLATING, "Comment translating")
        ]
        for stage, name in required_stages:
            progress = task.stage_progress.get(stage)
            if not progress or progress.status != StageStatus.COMPLETED:
                raise RuntimeError(f"{name} not completed for task {task.id}, can't synthesize")

        try:
            await self.task_manager.update_stage_status(
                task_id=task.id,
                stage=TaskStage.SYNTHESIZING,
                status=StageStatus.PROCESSING
            )
            # 获取所需文件路径
            download_progress = task.stage_progress.get(TaskStage.DOWNLOADING)
            translate_progress = task.stage_progress.get(TaskStage.TRANSLATING)
            split_progress = task.stage_progress.get(TaskStage.SUBTITLE_SPLITTING)

            video_path = Path(download_progress.output_files["video_path"])
            orig_srt = Path(split_progress.output_files["split_subtitle_path"])
            zh_srt = Path(split_progress.output_files["split_translated_subtitle_path"])

            # 获取翻译后的评论（含时间、作者、translated_text）
            from src.core.database import db_manager
            db = db_manager.get_database()
            col = db.comments
            comments = list(col.find({"video_url": task.video_url, "translated_text": {"$exists": True}}))
            comments.sort(key=lambda c: c.get("like_count", 0), reverse=True)
            comments = comments[:10]

            # 调用SyntheticVideo类进行合成
            from src.modules.synthetic_video import SyntheticVideo
            task_dir = Path(task.temp_dir)
            synth = SyntheticVideo(
                video_path=video_path,
                orig_subtitle_path=orig_srt,
                zh_subtitle_path=zh_srt,
                comments=comments,
                output_dir=task_dir,
                comment_style={
                    "bg": "transparent",
                    "font_outline": True,
                    "in_anim": "fadeInUp",
                    "out_anim": "fadeOutDown"
                }
            )
            output_path = await synth.synthesize()
            await self.task_manager.save_task(task)
            outputs = {"output_path": str(output_path)}
            logger.info(f"🎬 合成视频完成: {output_path}")
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