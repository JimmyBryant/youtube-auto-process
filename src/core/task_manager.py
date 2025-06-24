import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pymongo import ReturnDocument
from pymongo.errors import PyMongoError
from bson import ObjectId
import logging
from .database import db_manager
from .models import TaskStatus, TaskModel, TaskProgress, TaskStage, StageStatus, ProcessingType
from .exceptions import TaskNotFoundError, TaskStateError

class TaskManager:
    """任务管理中心（兼容Pydantic V2）"""
    
    def __init__(self):
        self.db = db_manager.get_database()
        self.task_collection = self.db.tasks
        self.logger = logging.getLogger('task_manager')
        self._ensure_indexes()

    def _ensure_indexes(self):
        """确保必要的索引存在"""
        try:
            self.task_collection.create_index([
                ('status', 1),
                ('priority', -1),
                ('created_at', 1)
            ], background=True)
            self.logger.debug("Database indexes verified")
        except Exception as e:
            self.logger.error("Index creation failed", exc_info=True)
            raise

    async def create_task(self, video_url: str, metadata: Optional[Dict] = None, priority: int = 1) -> str:
        """创建新任务"""
        task = TaskModel(
            video_url=video_url,
            metadata=metadata or {},
            status=TaskStatus.PENDING,
            priority=priority
        )
        
        try:
            result = self.task_collection.insert_one(task.model_dump())
            self.logger.info(f"Created task {result.inserted_id}")
            return str(result.inserted_id)
        except Exception as e:
            self.logger.error(f"Task creation failed: {str(e)}", exc_info=True)
            raise TaskStateError("Failed to create task")

    async def get_task(self, task_id: str) -> TaskModel:
        """获取任务详情"""
        try:
            doc = self.task_collection.find_one({'_id': ObjectId(task_id)})
            if not doc:
                raise TaskNotFoundError(f"Task {task_id} not found")
            return TaskModel(**doc)
        except PyMongoError as e:
            self.logger.error(f"Database error: {str(e)}")
            raise

    async def save_task(self, task: TaskModel) -> TaskModel:
        """保存整个任务"""
        try:
            updated = self.task_collection.find_one_and_update(
                {'_id': ObjectId(task.id)},
                {'$set': task.model_dump()},
                return_document=ReturnDocument.AFTER
            )
            if not updated:
                raise TaskNotFoundError(f"Task {task.id} not found")
            return TaskModel(**updated)
        except PyMongoError as e:
            self.logger.error(f"Save task failed: {str(e)}")
            raise TaskStateError("Failed to save task")

    async def update_task_status(self, task_id: str, status: TaskStatus, error: Optional[str] = None) -> TaskModel:
        """更新任务状态"""
        update_data = {
            'status': status.value,
            'updated_at': datetime.now(timezone.utc)
        }
        if error:
            update_data['error'] = error
        
        return await self._atomic_update(task_id, update_data)

    async def update_stage_status(
        self,
        task_id: str,
        stage: TaskStage,
        status: StageStatus,
        error: Optional[str] = None,
        output_files: Optional[Dict[str, str]] = None
    ) -> TaskModel:
        """更新任务的阶段状态"""
        now = datetime.now(timezone.utc)
        stage_progress_update = {
            "status": status.value,
            "updated_at": now
        }
        
        if status == StageStatus.PROCESSING:
            stage_progress_update["started_at"] = now
        elif status in (StageStatus.COMPLETED, StageStatus.FAILED):
            stage_progress_update["completed_at"] = now
            
        if error:
            stage_progress_update["error"] = error
        if output_files:
            stage_progress_update["output_files"] = output_files
            
        update_data = {
            f"stage_progress.{stage.value}": stage_progress_update,
            "updated_at": now
        }
        
        return await self._atomic_update(task_id, update_data)

    async def _atomic_update(self, task_id: str, update: Dict[str, Any]) -> TaskModel:
        """原子化更新操作"""
        try:
            updated = self.task_collection.find_one_and_update(
                {'_id': ObjectId(task_id)},
                {'$set': update},
                return_document=ReturnDocument.AFTER
            )
            if not updated:
                raise TaskNotFoundError(f"Task {task_id} not found")
            
            self.logger.debug(f"Task {task_id} updated: {update.keys()}")
            return TaskModel(**updated)
        except PyMongoError as e:
            self.logger.error(f"Update failed: {str(e)}")
            raise TaskStateError("Failed to update task state")

    async def list_tasks(self, status: Optional[TaskStatus] = None, limit: int = 100) -> list[TaskModel]:
        """查询任务列表"""
        query = {}
        if status:
            query['status'] = status.value
        cursor = self.task_collection.find(query).sort('created_at', -1).limit(limit)
        return [TaskModel(**doc) for doc in cursor]