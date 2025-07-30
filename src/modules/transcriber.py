import whisperx
from pathlib import Path
from typing import Optional
import logging
from src.core.task_manager import TaskManager

class AudioTranscriber:
    """基于Whisper的智能语音转写"""
    
    def __init__(self, model_size: str = "medium", device: str = "cpu"):
        self.logger = logging.getLogger('transcriber')
        self.device = device
        self.model = whisperx.load_model(model_size, device)

    async def transcribe(self, task_id: str, video_path: Path) -> Optional[Path]:
        """执行语音转写（使用 whisperx medium）"""
        try:
            srt_path = video_path.with_suffix('.srt')
            if not srt_path.exists():
                self.logger.info(f"开始转写: {video_path.name}")
                audio = whisperx.load_audio(str(video_path))
                result = self.model.transcribe(audio, batch_size=16)
                segments = result["segments"] if "segments" in result else result.get("segments", [])
                self._save_srt(segments, srt_path)
            await TaskManager().update_artifacts(task_id, {
                'srt_path': str(srt_path)
            })
            return srt_path
        except Exception as e:
            await TaskManager().mark_failed(task_id, f"转写失败: {str(e)}")
            self.logger.error(f"任务 {task_id} 转写失败: {str(e)}")
            return None

    def _save_srt(self, segments: list, path: Path, max_len: int = 40):
        """
        生成SRT字幕文件：
        - 每条字幕只一行
        - 超长字幕自动分割为多条，优先按标点/空格分割，时间轴均分
        """
        import re
        srt_idx = 1
        with open(path, 'w') as f:
            for seg in segments:
                start_time = seg['start']
                end_time = seg['end']
                text = seg['text'].replace('\n', ' ').replace('\r', ' ').replace('  ', ' ').strip()
                # 分割超长字幕
                split_texts = self._split_text(text, max_len)
                n = len(split_texts)
                # 均分时间轴
                duration = (end_time - start_time) / n if n > 0 else 0
                for i, sub_text in enumerate(split_texts):
                    sub_start = start_time + i * duration
                    sub_end = start_time + (i + 1) * duration if i < n - 1 else end_time
                    f.write(f"{srt_idx}\n{self._format_time(sub_start)} --> {self._format_time(sub_end)}\n{sub_text}\n\n")
                    srt_idx += 1

    def _split_text(self, text: str, max_len: int) -> list:
        """
        按标点、空格优先分割超长字幕，保证每条不超过 max_len 字符。
        """
        import re
        # 中文/英文标点优先
        punct = r'[。！？!?;；,，、]'  # 可根据需要扩展
        result = []
        buf = ''
        for char in text:
            buf += char
            if len(buf) >= max_len:
                # 尝试向前找到最近的标点或空格
                m = re.search(r'(.+?)([。！？!?;；,，、\s])[^。！？!?;；,，、\s]*$', buf)
                if m:
                    cut = m.end()
                    result.append(buf[:cut].strip())
                    buf = buf[cut:]
                else:
                    result.append(buf.strip())
                    buf = ''
        if buf.strip():
            result.append(buf.strip())
        # 二次处理，防止有的分段仍超长
        final = []
        for seg in result:
            if len(seg) <= max_len:
                final.append(seg)
            else:
                # 强制截断
                for i in range(0, len(seg), max_len):
                    final.append(seg[i:i+max_len])
        return final

    @staticmethod
    def _format_time(seconds: float) -> str:
        """秒数转SRT时间格式"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds = seconds % 60
        return f"{hours:02}:{minutes:02}:{seconds:06.3f}".replace('.', ',')