from playwright.async_api import async_playwright
from pathlib import Path
import json
from typing import Optional
import logging
from src.core.models import TaskModel
from src.core.task_manager import TaskManager

class CommentScraper:
    """基于Playwright的智能评论爬取"""
    
    def __init__(self):
        self.logger = logging.getLogger('scraper')
        self.browser = None

    async def scrape(self, task_id: str, max_comments: int = 100) -> Optional[Path]:
        """执行评论爬取"""
        task = await TaskManager().get_task(task_id)
        
        try:
            async with async_playwright() as p:
                self.browser = await p.chromium.launch(headless=True)
                page = await self.browser.new_page()
                
                await page.goto(task.video_url)
                await page.wait_for_selector('div#comments', timeout=10000)
                
                comments = []
                for _ in range(max_comments // 20):
                    batch = await self._extract_comments(page)
                    comments.extend(batch)
                    if not await self._scroll_to_next(page):
                        break
                
                output_path = Path(f"data/comments/{task_id}.json")
                output_path.parent.mkdir(exist_ok=True)
                output_path.write_text(json.dumps(comments, ensure_ascii=False))
                
                await TaskManager().update_artifacts(task_id, {
                    'comments_path': str(output_path)
                })
                return output_path
                
        except Exception as e:
            await TaskManager().mark_failed(task_id, f"爬取失败: {str(e)}")
            self.logger.error(f"任务 {task_id} 爬取失败: {str(e)}")
            return None

    async def _extract_comments(self, page) -> list:
        """提取当前页面的评论"""
        return await page.evaluate('''() => {
            return Array.from(document.querySelectorAll('.comment-item'))
                .map(item => ({
                    author: item.querySelector('.author').innerText,
                    content: item.querySelector('.content').innerText,
                    likes: parseInt(item.querySelector('.like-count').innerText) || 0
                }))
        }''')

    async def _scroll_to_next(self, page) -> bool:
        """滚动加载更多评论"""
        return await page.evaluate('''async () => {
            const loader = document.querySelector('.comments-loader');
            if (loader) {
                loader.click();
                await new Promise(r => setTimeout(r, 2000));
                return true;
            }
            return false;
        }''')