from openai import AsyncOpenAI
from pathlib import Path
from typing import Optional
import logging
from src.core.models import TaskModel
from src.core.task_manager import TaskManager

class SubtitleTranslator:
    """基于GPT的多语言字幕翻译"""
    
    def __init__(self):
        self.client = AsyncOpenAI()
        self.logger = logging.getLogger('translator')

    async def translate(self, task_id: str, target_lang: str = "zh") -> Optional[Path]:
        """执行字幕翻译"""
        task = await TaskManager().get_task(task_id)
        
        try:
            srt_path = Path(task.artifacts['srt_path'])
            translated_path = srt_path.with_stem(f"{srt_path.stem}_{target_lang}")
            
            if not translated_path.exists():
                self.logger.info(f"开始翻译: {srt_path.name}")
                with open(srt_path) as f:
                    content = f.read()
                
                response = await self.client.chat.completions.create(
                    model="gpt-4",
                    messages=[
                        {"role": "system", "content": "专业字幕翻译"},
                        {"role": "user", "content": f"翻译到{target_lang}:\n\n{content}"}
                    ]
                )
                
                translated_text = response.choices[0].message.content
                translated_path.write_text(translated_text)
                
            await TaskManager().update_artifacts(task_id, {
                f"{target_lang}_srt_path": str(translated_path)
            })
            return translated_path
            
        except Exception as e:
            await TaskManager().mark_failed(task_id, f"翻译失败: {str(e)}")
            self.logger.error(f"任务 {task_id} 翻译失败: {str(e)}")
            return None