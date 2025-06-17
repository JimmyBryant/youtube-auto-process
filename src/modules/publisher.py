from playwright.async_api import async_playwright
from pathlib import Path
from typing import Optional
import logging
from src.core.models import TaskModel
from src.core.task_manager import TaskManager

class VideoPublisher:
    """多平台视频发布控制器"""
    
    def __init__(self):
        self.logger = logging.getLogger('publisher')
        self.browser = None

    async def publish(self, task_id: str, platform: str = "bilibili") -> Optional[str]:
        """执行视频发布"""
        task = await TaskManager().get_task(task_id)
        
        try:
            async with async_playwright() as p:
                self.browser = await p.chromium.launch(headless=False)
                context = await self._prepare_context(platform)
                page = await context.new_page()
                
                # 平台特定的发布流程
                if platform == "bilibili":
                    video_url = await self._publish_to_bilibili(page, task)
                elif platform == "youtube":
                    video_url = await self._publish_to_youtube(page, task)
                else:
                    raise ValueError(f"不支持的平台: {platform}")
                
                await TaskManager().update_artifacts(task_id, {
                    f"{platform}_url": video_url
                })
                return video_url
                
        except Exception as e:
            await TaskManager().mark_failed(task_id, f"{platform}发布失败: {str(e)}")
            self.logger.error(f"任务 {task_id} 发布失败: {str(e)}")
            return None

    async def _prepare_context(self, platform: str):
        """准备平台特定的浏览器上下文"""
        context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            storage_state=f"auth/{platform}_auth.json"
        )
        await context.add_cookies(self._load_cookies(platform))
        return context

    async def _publish_to_bilibili(self, page, task) -> str:
        """B站发布流程"""
        await page.goto("https://member.bilibili.com/platform/upload/video")
        await page.set_input_files('input[type="file"]', task.artifacts['video_path'])
        await page.fill('input[name="title"]', Path(task.artifacts['video_path']).stem)
        await page.click('.submit-btn')
        await page.wait_for_selector('.success-message', timeout=30000)
        return page.url

    async def _publish_to_youtube(self, page, task) -> str:
        """YouTube发布流程"""
        await page.goto("https://studio.youtube.com/upload")
        await page.set_input_files('input[type="file"]', task.artifacts['video_path'])
        await page.fill('text="Title"', Path(task.artifacts['video_path']).stem)
        await page.click('text="PUBLISH"')
        await page.wait_for_selector('.video-url', timeout=30000)
        return await page.evaluate('document.querySelector(".video-url").href')

    def _load_cookies(self, platform: str) -> list:
        """加载平台登录凭证"""
        # 需要预先配置认证信息
        return []