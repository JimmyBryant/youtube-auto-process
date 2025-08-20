import logging
from pathlib import Path
from typing import List, Dict, Optional
import asyncio
import json

logger = logging.getLogger('comment_processor')


async def fetch_comments(video_url: str, output_dir: Path, max_comments: int = 100) -> Path:
    MIN_COMMENTS = 10
    """
    使用 Playwright 获取 YouTube 视频全部评论（包括主评论和所有回复），自动点击“显示更多”与“更多回复”，所有字段必须有值。
    """
    try:
        logger.info(f"Fetching comments for video: {video_url}, max_comments={max_comments}")
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.error("Playwright 未安装，请先运行: pip install playwright && playwright install")
            raise RuntimeError("Playwright 未安装，请先运行: pip install playwright && playwright install")

        async def extract_comment(el):
            try:
                # 主评论内容
                content = await el.query_selector('yt-attributed-string#content-text')
                text = await content.inner_text() if content else ""
                like_span = await el.query_selector('span#vote-count-middle')
                like_text = await like_span.inner_text() if like_span else "0"
                like_count = int(like_text.replace(',', '').strip() or "0")
                time_el = await el.query_selector('span#published-time-text a')
                timestamp = await time_el.inner_text() if time_el else ""
                author_el = await el.query_selector('a#author-text span')
                author = await author_el.inner_text() if author_el else ""
                avatar_el = await el.query_selector('img#img')
                avatar_url = await avatar_el.get_attribute('src') if avatar_el else ""
                comment = {
                    "author": author.strip(),
                    "avatar_url": avatar_url,
                    "text": text.strip(),
                    "like_count": like_count,
                    "timestamp": timestamp.strip()
                }
                print("[extract_comment]", json.dumps(comment, ensure_ascii=False))
                return comment
            except Exception as e:
                logger.warning(f"解析单条评论失败: {e}")
                return None

        async def extract_comment_with_timeout(el, timeout=1.0):
            try:
                return await asyncio.wait_for(extract_comment(el), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("解析单条评论超时，已跳过。")
                return None
            except Exception as e:
                logger.warning(f"解析单条评论异常: {e}")
                return None

        async def slow_scroll_to_bottom(page, max_scroll=100, sleep_time=1.2):
            # 缓慢滚动页面到底部，确保评论区全部加载
            # 先滚动到评论区顶部，确保评论区在视野内
            await page.evaluate('''
                const comments = document.querySelector('#comments');
                if (comments) comments.scrollIntoView({behavior: 'smooth', block: 'start'});
            ''')
            await asyncio.sleep(1.2)
            last_height = await page.evaluate('document.documentElement.scrollHeight')
            last_count = await page.evaluate('''() => document.querySelectorAll('ytd-comment-thread-renderer').length''')
            scroll_count = 0
            no_change_rounds = 0
            max_no_change = 3
            while scroll_count < max_scroll:
                await page.mouse.wheel(0, 800)
                await asyncio.sleep(sleep_time)
                new_height = await page.evaluate('document.documentElement.scrollHeight')
                new_count = await page.evaluate('''() => document.querySelectorAll('ytd-comment-thread-renderer').length''')
                if new_count == last_count:
                    no_change_rounds += 1
                else:
                    no_change_rounds = 0
                if no_change_rounds >= max_no_change:
                    logger.info(f"评论区滚动{scroll_count}次后，评论数连续{max_no_change}次未增加，判定已加载完毕。")
                    break
                last_height = new_height
                last_count = new_count
                scroll_count += 1
            logger.info(f"已缓慢滚动{scroll_count}次，页面高度: {last_height}，主评论数: {last_count}")

        async def expand_all_comments(page):
            # 先缓慢滚动到底部，确保评论加载
            await slow_scroll_to_bottom(page)
            # 再点击“显示更多”与“更多回复”
            last_count = 0
            scroll_count = 0
            max_scroll = 30
            while scroll_count < max_scroll:
                more_btns = await page.query_selector_all('ytd-button-renderer#more-replies, ytd-button-renderer#more-replies-sub-thread, ytd-button-renderer#more')
                for btn in more_btns:
                    try:
                        btn_el = await btn.query_selector('button')
                        if btn_el:
                            await btn_el.click()
                            await asyncio.sleep(0.5)
                    except Exception:
                        continue
                comment_elements = await page.query_selector_all('ytd-comment-thread-renderer')
                if len(comment_elements) == last_count:
                    break
                last_count = len(comment_elements)
                scroll_count += 1
            logger.info(f"已点击更多按钮{scroll_count}次，主评论数: {last_count}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()
            try:
                await page.goto(video_url)
                await page.wait_for_selector('#comments', timeout=60000)

                # 只滚动加载主评论，不展开任何折叠
                await slow_scroll_to_bottom(page)

                # 获取所有主评论
                comment_elements = await page.query_selector_all('ytd-comment-thread-renderer')
                logger.info(f"开始抓取主评论，共{len(comment_elements)}条")
                comments = []
                for idx, el in enumerate(comment_elements):
                    logger.info(f"[主循环] idx={idx+1}/{len(comment_elements)}: 开始处理ElementHandle={el}")
                    try:
                        c = await extract_comment_with_timeout(el)
                        logger.info(f"[主循环] idx={idx+1}: extract_comment_with_timeout返回: {c}")
                        if c:
                            comments.append(c)
                    except Exception as e:
                        logger.error(f"[主循环] idx={idx+1}: 处理主评论异常: {e}")

                # 只保留所有属性都不为空的评论
                required_keys = ["author", "avatar_url", "text", "like_count", "timestamp"]
                filtered_comments = [c for c in comments if all(c.get(k) not in (None, "") for k in required_keys)]
                if not filtered_comments:
                    logger.warning("所有评论属性不全，未能获取有效评论！")
                    raise RuntimeError("未能获取有效评论，任务失败")
                logger.info(f"过滤后有效评论数: {len(filtered_comments)}")
                for idx, c in enumerate(filtered_comments):
                    print(f"评论{idx+1}:")
                    print(json.dumps(c, ensure_ascii=False, indent=2))
                # 按点赞数排序，取前 max_comments 条
                filtered_comments.sort(key=lambda x: x.get('like_count', 0), reverse=True)
                filtered_comments = filtered_comments[:max_comments]
                # 检查评论数量是否达标，不足则报错，不保存
                if len(filtered_comments) < MIN_COMMENTS:
                    logger.error(f"有效评论数不足{MIN_COMMENTS}，仅获取到{len(filtered_comments)}条，任务失败！")
                    raise RuntimeError(f"有效评论数不足{MIN_COMMENTS}，任务失败")
            finally:
                try:
                    await browser.close()
                except Exception as e:
                    logger.error(f"关闭浏览器失败: {e}")

        # 保存到本地
        comments_path = output_dir / "comments.json"
        with open(comments_path, 'w', encoding='utf-8') as f:
            json.dump(filtered_comments, f, ensure_ascii=False, indent=2)
        logger.info(f"Comments saved at {comments_path}")

        # 保存到数据库（MongoDB）
        try:
            from src.core.database import db_manager
            db = db_manager.get_database()
            col = db.comments
            for c in filtered_comments:
                c['video_url'] = video_url
            if filtered_comments:
                col.insert_many(filtered_comments)
                logger.info(f"Inserted {len(filtered_comments)} comments to DB")
        except Exception as e:
            logger.warning(f"保存评论到数据库失败: {e}")

        return comments_path
    except Exception as e:
        logger.error(f"Failed to fetch comments: {str(e)}")
        raise

async def process_comments(comments_file: Path, output_dir: Path) -> List[Path]:
    """
    处理评论并生成可视化内容
    :param comments_file: 评论文件路径
    :param output_dir: 输出目录
    :return: 生成的图片文件路径列表
    """
    try:
        logger.info(f"Processing comments from {comments_file.name}")
        
        # 模拟处理过程
        await asyncio.sleep(3)  # 模拟处理时间
        
        # 读取评论数据
        with open(comments_file, 'r', encoding='utf-8') as f:
            comments = json.load(f)
        
        # 生成评论图片
        image_paths = []
        for i, comment in enumerate(comments[:3]):  # 只处理前3条评论作为示例
            img_path = output_dir / f"comment_{i+1}.png"
            
            # 模拟生成图片文件
            with open(img_path, 'wb') as f:
                f.write(b'')  # 实际项目中这里会生成真正的图片
            
            image_paths.append(img_path)
        
        logger.info(f"Generated {len(image_paths)} comment images")
        return image_paths
        
    except Exception as e:
        logger.error(f"Failed to process comments: {str(e)}")
        raise