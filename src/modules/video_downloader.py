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
        # 检查 yt_dlp 是否为最新版，不是则自动更新
        try:
            import subprocess
            import pkg_resources
            latest_version = None
            try:
                # 获取 PyPI 上的最新版本
                import json, urllib.request
                with urllib.request.urlopen('https://pypi.org/pypi/yt-dlp/json', timeout=5) as resp:
                    data = json.load(resp)
                    latest_version = data['info']['version']
            except Exception as e:
                logger.warning(f"获取 yt-dlp 最新版本失败: {e}")
            try:
                current_version = pkg_resources.get_distribution('yt-dlp').version
            except Exception as e:
                logger.warning(f"获取 yt-dlp 当前版本失败: {e}")
                current_version = None
            if latest_version and current_version and latest_version != current_version:
                logger.info(f"检测到 yt-dlp 不是最新版({current_version} -> {latest_version})，自动更新...")
                subprocess.run(['python3', '-m', 'pip', 'install', '--upgrade', 'yt-dlp'], check=False)
            elif latest_version and current_version:
                logger.info(f"yt-dlp 已是最新版: {current_version}")
        except Exception as e:
            logger.warning(f"自动检查/更新 yt-dlp 失败: {e}")

    async def download(self, video_url: str, output_dir: Path, cookie_file: Path = None) -> Tuple[Path, Path]:
        """
        下载视频和封面
        参数:
            video_url: 视频URL
            output_dir: 输出目录(Path对象)
            cookie_file: 可选，cookie文件路径
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
            if cookie_file:
                ydl_opts['cookiefile'] = str(cookie_file)
            
            # 使用线程池执行阻塞的下载操作
            loop = asyncio.get_running_loop()
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = await loop.run_in_executor(None, ydl.extract_info, video_url, True)
                
                video_path = output_dir / f"{task_id}.mp4"
                # 支持多种图片格式
                thumb_path = None
                for ext in ["jpg", "webp", "png", "jpeg"]:
                    candidate = output_dir / f"{task_id}.{ext}"
                    if candidate.exists():
                        thumb_path = candidate
                        break
                # 确保视频文件存在
                if not video_path.exists():
                    raise FileNotFoundError(f"下载的视频文件不存在: {video_path}")
                if not thumb_path:
                    logger.warning(f"缩略图文件不存在: {output_dir}/{task_id}.[jpg|webp|png|jpeg]")
                logger.info(f"下载完成: {video_path}")
                return video_path, thumb_path
            
        except Exception as e:
            logger.error(f"下载失败: {str(e)}")
            raise RuntimeError(f"视频下载失败: {str(e)}")

# 全局实例（可选）
downloader = VideoDownloader()

async def download_video(video_url: str, output_dir: Path, cookie_file: Path = None) -> Tuple[Path, Path]:
    """兼容旧版的快捷函数，支持 cookie"""
    return await downloader.download(video_url, output_dir, cookie_file)