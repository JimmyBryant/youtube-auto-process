import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from .task_manager import TaskManager
from .models import TaskStatus, TaskModel

logger = logging.getLogger('task_scheduler')

class TaskScheduler:
    """任务调度器，负责管理和调度视频处理任务"""
    
    def __init__(self, max_concurrent_tasks: int = 3):
        self.task_manager = TaskManager()
        self.max_concurrent_tasks = max_concurrent_tasks
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self._stop_event = asyncio.Event()
        
    async def start(self) -> None:
        """启动任务调度器"""
        logger.info("Starting task scheduler...")
        try:
            while not self._stop_event.is_set():
                await self._schedule_tasks()
                await asyncio.sleep(5)  # 每5秒检查一次
        except asyncio.CancelledError:
            logger.info("Task scheduler received cancel signal")
        except Exception as e:
            logger.error(f"Scheduler error: {str(e)}", exc_info=True)
            raise
        finally:
            await self._cleanup()
            
    async def stop(self) -> None:
        """停止任务调度器"""
        self._stop_event.set()
        logger.info("Stopping task scheduler...")
        
    async def _schedule_tasks(self) -> None:
        """调度待处理任务"""
        # 获取待处理任务
        pending_tasks = await self.task_manager.list_tasks(status=TaskStatus.PENDING)
        
        # 检查当前活跃任务数量
        running_count = len(self.active_tasks)
        available_slots = self.max_concurrent_tasks - running_count
        
        if available_slots <= 0:
            logger.debug(f"No available slots (running: {running_count})")
            return
            
        # 选择优先级高的任务
        tasks_to_run = sorted(
            pending_tasks,
            key=lambda t: (-t.priority, t.timestamps["created_at"])
        )[:available_slots]
        
        # 启动新任务
        for task in tasks_to_run:
            logger.info(f"Starting task {task.id} (priority: {task.priority})")
            task_task = asyncio.create_task(self._process_task(task))
            self.active_tasks[task.id] = task_task
            task_task.add_done_callback(lambda t, task_id=task.id: self._task_done_callback(t, task_id))
            
    async def _process_task(self, task: TaskModel) -> None:
        """处理单个任务"""
        try:
            # 更新任务状态为处理中
            await self.task_manager.update_task_status(task.id, TaskStatus.PROCESSING)
            
            # 这里应该调用实际的视频处理逻辑
            # 例如: await VideoProcessor().process(task)
            
            # 模拟处理延迟
            await asyncio.sleep(10)
            
            # 标记任务完成
            await self.task_manager.update_task_status(task.id, TaskStatus.COMPLETED)
            logger.info(f"Task {task.id} completed successfully")
            
        except Exception as e:
            logger.error(f"Task {task.id} failed: {str(e)}", exc_info=True)
            await self.task_manager.update_task_status(task.id, TaskStatus.FAILED, error=str(e))
            
    def _task_done_callback(self, task: asyncio.Task, task_id: str) -> None:
        """任务完成回调"""
        self.active_tasks.pop(task_id, None)
        if task.exception():
            logger.error(f"Task {task_id} raised an exception", exc_info=task.exception())
            
    async def _cleanup(self) -> None:
        """清理资源"""
        # 取消所有正在运行的任务
        for task_id, task in self.active_tasks.items():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                    
        self.active_tasks.clear()
        logger.info("Task scheduler cleanup completed")
        
    async def monitor_status(self) -> None:
        """监控任务状态"""
        while not self._stop_event.is_set():
            logger.info(f"Current status - Active tasks: {len(self.active_tasks)}")
            pending = await self.task_manager.list_tasks(status=TaskStatus.PENDING)
            logger.info(f"Pending tasks: {len(pending)}")
            await asyncio.sleep(30)