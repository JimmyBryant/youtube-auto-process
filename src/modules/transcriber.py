import whisper
from pathlib import Path
from typing import Optional
import logging
from core import TaskManager, TaskModel

class AudioTranscriber:
    """基于Whisper的智能语音转写"""
    
    def __init__(self, model_size: str = "base"):
        self.logger = logging.getLogger('transcriber')
        self.model = whisper.load_model(model_size)

    async def transcribe(self, task_id: str) -> Optional[Path]:
        """执行语音转写"""
        task = await TaskManager().get_task(task_id)
        
        try:
            video_path = Path(task.artifacts['video_path'])
            srt_path = video_path.with_suffix('.srt')
            
            if not srt_path.exists():
                self.logger.info(f"开始转写: {video_path.name}")
                result = self.model.transcribe(str(video_path))
                self._save_srt(result['segments'], srt_path)
                
            await TaskManager().update_artifacts(task_id, {
                'srt_path': str(srt_path)
            })
            return srt_path
            
        except Exception as e:
            await TaskManager().mark_failed(task_id, f"转写失败: {str(e)}")
            self.logger.error(f"任务 {task_id} 转写失败: {str(e)}")
            return None

    def _save_srt(self, segments: list, path: Path):
        """生成SRT字幕文件"""
        with open(path, 'w') as f:
            for i, seg in enumerate(segments, start=1):
                start = self._format_time(seg['start'])
                end = self._format_time(seg['end'])
                f.write(f"{i}\n{start} --> {end}\n{seg['text']}\n\n")

    @staticmethod
    def _format_time(seconds: float) -> str:
        """秒数转SRT时间格式"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds = seconds % 60
        return f"{hours:02}:{minutes:02}:{seconds:06.3f}".replace('.', ',')