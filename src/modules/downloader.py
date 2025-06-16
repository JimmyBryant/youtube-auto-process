import subprocess
from pathlib import Path
from typing import Optional
import logging
from tqdm import tqdm
from yt_dlp import YoutubeDL
from core import TaskManager, TaskModel

class VideoDownloader:
    """支持4K/HD的智能视频下载器"""
    
    def __init__(self):
        self.logger = logging.getLogger('downloader')
        self.ydl_opts = {
            'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]',
            'outtmpl': '%(title)s.%(ext)s',
            'progress_hooks': [self._progress_hook],
            'noplaylist': True
        }

    async def download(self, task_id: str) -> Optional[Path]:
        """执行视频下载"""
        task = await TaskManager().get_task(task_id)
        
        try:
            with YoutubeDL(self.ydl_opts) as ydl:
                info = ydl.extract_info(task.video_url, download=False)
                filepath = Path(ydl.prepare_filename(info))
                
                if not filepath.exists():
                    self.logger.info(f"开始下载: {task.video_url}")
                    ydl.download([task.video_url])
                
                await TaskManager().update_artifacts(task_id, {
                    'video_path': str(filepath),
                    'resolution': info.get('resolution', 'unknown')
                })
                return filepath
                
        except Exception as e:
            await TaskManager().mark_failed(task_id, f"下载失败: {str(e)}")
            self.logger.error(f"任务 {task_id} 下载失败: {str(e)}")
            return None

    def _progress_hook(self, d: dict):
        """下载进度回调"""
        if d['status'] == 'downloading':
            tqdm.write(f"下载进度: {d['_percent_str']} | 速度: {d['_speed_str']}")

    @classmethod
    def get_available_formats(cls, url: str) -> list:
        """获取视频可用格式"""
        with YoutubeDL({'quiet': True}) as ydl:
            info = ydl.extract_info(url, download=False)
            return info.get('formats', [])