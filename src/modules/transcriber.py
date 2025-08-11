
import whisperx
from pathlib import Path
from typing import Optional
import logging
from src.core.task_manager import TaskManager
from src.core.models import TaskStatus

class AudioTranscriber:
    """基于Whisper的智能语音转写"""
    
    def __init__(self, model_size: str = "medium", device: str = "cpu"):
        self.logger = logging.getLogger('transcriber')
        self.device = device
        # 强制使用 float32，避免 float16 错误
        self.model = whisperx.load_model(model_size, device, compute_type="float32")

    async def transcribe(self, task_id: str, video_path: Path) -> Optional[Path]:
        """执行语音转写（使用 whisperx medium）"""
        import time
        try:
            srt_path = video_path.with_suffix('.srt')
            if not srt_path.exists():
                self.logger.info(f"开始转写: {video_path.name}")
                audio = whisperx.load_audio(str(video_path))
                # 获取音频时长
                try:
                    import soundfile as sf
                    f = sf.SoundFile(str(video_path))
                    audio_duration = f.frames / f.samplerate
                except Exception:
                    audio_duration = None
                start_time = time.time()
                result = self.model.transcribe(audio, batch_size=16)
                end_time = time.time()
                segments = result["segments"] if "segments" in result else result.get("segments", [])
                self._save_srt(segments, srt_path)
                # 打印音频时长和转录耗时
                if audio_duration is not None:
                    self.logger.info(f"音频时长: {audio_duration:.2f} 秒")
                self.logger.info(f"转录耗时: {end_time - start_time:.2f} 秒")
            from src.core.models import TaskStage, StageStatus
            await TaskManager().update_stage_status(
                task_id,
                stage=TaskStage.TRANSCRIBING,
                status=StageStatus.COMPLETED,
                output_files={'subtitle_path': str(srt_path)}
            )
            return srt_path
        except Exception as e:
            await TaskManager().update_task_status(task_id, status=TaskStatus.FAILED, error=f"转写失败: {str(e)}")
            self.logger.error(f"任务 {task_id} 转写失败: {str(e)}")
            return None

    def _save_srt(self, segments: list, path: Path, max_len: int = 40):
        """
        生成SRT字幕文件：每个 segment 只输出一条 SRT，不做切割，保留原始分段。
        """
        srt_idx = 1
        with open(path, 'w') as f:
            for seg in segments:
                start_time = seg['start']
                end_time = seg['end']
                text = seg['text'].replace('\n', ' ').replace('\r', ' ').replace('  ', ' ').strip()
                f.write(f"{srt_idx}\n{self._format_time(start_time)} --> {self._format_time(end_time)}\n{text}\n\n")
                srt_idx += 1

    def _split_text(self, text: str, max_len: int) -> list:
        """
        优化字幕切割：
        1. 先按句末标点（。！？.!?…）分割
        2. 超长句再按逗号、分号、顿号等断句标点分割
        3. 仍超长则按空格优雅切割
        4. 实在不行再硬切
        """
        import re
        # 1. 先按句末标点分割
        end_punct = r'[。！？.!?…]'  # 句末标点
        parts = re.split(f'({end_punct})', text)
        sents = []
        buf = ''
        for seg in parts:
            if not seg:
                continue
            buf += seg
            if re.match(end_punct, seg):
                sents.append(buf.strip())
                buf = ''
        if buf.strip():
            sents.append(buf.strip())

        def further_split(sent, max_len):
            sent = sent.strip()
            if len(sent) <= max_len:
                return [sent]
            # 2. 按逗号、分号、顿号等断句标点分割
            mid_punct = r'[，,；;、]'  # 断句标点
            mid_parts = re.split(f'({mid_punct})', sent)
            mid_sents = []
            mid_buf = ''
            for seg in mid_parts:
                if not seg:
                    continue
                mid_buf += seg
                if re.match(mid_punct, seg):
                    mid_sents.append(mid_buf.strip())
                    mid_buf = ''
            if mid_buf.strip():
                mid_sents.append(mid_buf.strip())
            # 如果分割后都不超长，直接返回
            if all(len(s) <= max_len for s in mid_sents):
                return mid_sents
            # 3. 对超长的再按空格优雅切割
            result = []
            for s in mid_sents:
                if len(s) <= max_len:
                    result.append(s)
                else:
                    # 优雅按空格分割
                    words = s.split(' ')
                    lines = []
                    line = ''
                    for word in words:
                        if not line:
                            line = word
                        elif len(line) + 1 + len(word) <= max_len:
                            line += ' ' + word
                        else:
                            lines.append(line)
                            line = word
                    if line:
                        lines.append(line)
                    # 递归处理每一行，确保没有超长
                    for l in lines:
                        if len(l) <= max_len:
                            result.append(l)
                        else:
                            # 4. 实在不行再硬切
                            for i in range(0, len(l), max_len):
                                result.append(l[i:i+max_len])
            return result

        final = []
        for seg in sents:
            final.extend(further_split(seg, max_len))
        return final

    @staticmethod
    def _format_time(seconds: float) -> str:
        """秒数转SRT时间格式"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds = seconds % 60
        return f"{hours:02}:{minutes:02}:{seconds:06.3f}".replace('.', ',')