# src/modules/video_downloader.py
import asyncio
import os
import logging
from pathlib import Path
from typing import Tuple
import yt_dlp

logger = logging.getLogger('video_downloader')

class VideoDownloader:
    """视频下载模块（支持断点续传）"""
    
    def __init__(self):
        pass  # 不再需要固定临时目录

    async def download(self, video_url: str, output_dir: Path) -> Tuple[Path, Path]:
        """
        下载视频和封面
        参数:
            video_url: 视频URL
            output_dir: 输出目录(Path对象)
        返回: (视频路径, 封面路径)
        """
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
            task_id = output_dir.name  # 使用目录名作为任务ID
            
            ydl_opts = {
                'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best',
                'outtmpl': str(output_dir / f"{task_id}.%(ext)s"),
                'writethumbnail': True,
                'quiet': True,
            }
            
            # 使用线程池执行阻塞的下载操作
            loop = asyncio.get_running_loop()
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = await loop.run_in_executor(None, ydl.extract_info, video_url, True)
                
                video_path = output_dir / f"{task_id}.mp4"
                thumb_path = output_dir / f"{task_id}.jpg"
                
                # 确保文件存在
                if not video_path.exists():
                    raise FileNotFoundError(f"下载的视频文件不存在: {video_path}")
                if not thumb_path.exists():
                    logger.warning(f"缩略图文件不存在: {thumb_path}")
                    thumb_path = None
                
            logger.info(f"下载完成: {video_path}")
            return video_path, thumb_path
            
        except Exception as e:
            logger.error(f"下载失败: {str(e)}")
            raise RuntimeError(f"视频下载失败: {str(e)}")

# 全局实例（可选）
downloader = VideoDownloader()

async def download_video(video_url: str, output_dir: Path) -> Tuple[Path, Path]:
    """兼容旧版的快捷函数"""
    return await downloader.download(video_url, output_dir)