# src/database.py
import os
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import logging

class MongoDBManager:
    """安全的MongoDB连接管理器（单例模式）"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_db()
        return cls._instance
    
    def _init_db(self):
        """初始化数据库连接"""
        self.logger = logging.getLogger('mongodb')
        
        # 从环境变量读取配置（带默认值）
        self.host = os.getenv('MONGO_HOST', 'localhost')
        self.port = int(os.getenv('MONGO_PORT', '27017'))
        self.db_name = os.getenv('MONGO_DB_NAME', 'video_processor')
        self.username = os.getenv('MONGO_USER')
        self.password = os.getenv('MONGO_PASSWORD')
        
        try:
            self.client = self._create_client()
            self.client.admin.command('ping')  # 测试连接
            self.logger.info("MongoDB connection established")
        except PyMongoError as e:
            self.logger.error("MongoDB connection failed", exc_info=True)
            raise RuntimeError(f"Database connection error: {str(e)}")

    def _create_client(self) -> MongoClient:
        """创建MongoDB客户端"""
        if self.username and self.password:
            uri = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/?authSource=admin"
        else:
            uri = f"mongodb://{self.host}:{self.port}"
        
        return MongoClient(
            uri,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=3000,
            socketTimeoutMS=30000
        )

    def get_database(self):
        """获取数据库实例"""
        return self.client[self.db_name]

# 全局数据库管理器实例
db_manager = MongoDBManager()