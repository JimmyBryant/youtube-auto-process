import os
import logging
from pathlib import Path
from typing import List, Dict, Any
import asyncio

logger = logging.getLogger('synthetic_video')

class SyntheticVideo:
    """
    合成视频类：负责将原视频、原字幕、中文字幕和带动画的评论合成为最终视频。
    支持透明背景、描边、入出动画等评论样式参数。
    """
    def __init__(self,
                 video_path: Path,
                 orig_subtitle_path: Path,
                 zh_subtitle_path: Path,
                 comments: List[Dict[str, Any]],
                 output_dir: Path,
                 comment_style: Dict[str, Any] = None):
        self.video_path = video_path
        self.orig_subtitle_path = orig_subtitle_path
        self.zh_subtitle_path = zh_subtitle_path
        self.comments = comments
        self.output_dir = output_dir
        self.comment_style = comment_style or {}
    @staticmethod
    def find_chinese_font():
        """
        优先返回可用的中文字体名（而非路径），以提升ffmpeg兼容性。
        """
        # 优先字体名列表，按常见程度排序
        font_names = [
            # macOS/现代优先
            '苹方', 'PingFang SC', 'Hiragino Sans GB', '微软雅黑', 'Microsoft YaHei', '黑体', 'SimHei',
            'Noto Sans CJK SC', 'WenQuanYi Zen Hei', 'STHeiti',
            # 传统/不推荐
            '宋体', 'SimSun', 'STSong', 'MSYH'
        ]
        # 用fc-list查找系统可用字体名
        try:
            import subprocess
            output = subprocess.check_output(["fc-list", ":lang=zh", "family"], text=True, errors="ignore")
            available_fonts = set()
            for line in output.splitlines():
                # 可能有多个字体名，用逗号分隔
                for name in line.split(","):
                    available_fonts.add(name.strip())
            for fname in font_names:
                if fname in available_fonts:
                    logger.info(f"[SyntheticVideo] 检测到可用中文字体: {fname}")
                    return fname
        except Exception as e:
            logger.warning(f"[SyntheticVideo] fc-list 检查字体失败: {e}")
        # 回退到常见字体名
        logger.warning("[SyntheticVideo] 未找到常见中文字体，将使用 Arial，可能导致乱码。建议手动指定！")
        return 'Arial'

    async def synthesize(self) -> Path:
        """
        合成视频，插入多字幕和带动画的评论。
        :return: 合成后视频路径
        """
        import subprocess
        import tempfile
        import json
        from datetime import timedelta
        try:
            video_url = self.comments[0].get("video_url") if self.comments and "video_url" in self.comments[0] else ""
            logger.info(f"[SyntheticVideo][{video_url}] 开始合成: {self.video_path.name}, 评论数: {len(self.comments)}")
            output_path = self.output_dir / f"synthetic_{self.video_path.name}"

            # 1. 生成评论弹幕ASS字幕文件（顶部，无动画，蓝色字白描）
            ass_path = self.output_dir / f"comments.ass"
            self._generate_comments_ass(ass_path)

            # 2. ffmpeg合成：原视频+原字幕+中文字幕+评论ASS弹幕
            zh_font = self.find_chinese_font()
            en_font = 'Arial'
            vf_filters = []
            # 英文字幕底部（白色）
            vf_filters.append(
                f"subtitles='{self.orig_subtitle_path}':force_style='Fontname={en_font},Fontsize=20,PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,BorderStyle=1,Outline=2,Shadow=0,Alignment=2'"
            )
            # 中文字幕在英文字幕上方（金黄色，示例：&H0033CCFF）
            vf_filters.append(
                f"subtitles='{self.zh_subtitle_path}':force_style='Fontname={zh_font},Fontsize=24,PrimaryColour=&H0033CCFF,OutlineColour=&H00000000,BorderStyle=1,Outline=2,Shadow=0,Alignment=2,MarginV=30'"
            )
            # 评论弹幕ASS顶部（Alignment=8，MarginV=40）
            vf_filters.append(
                f"ass={ass_path}"
            )
            vf_str = ",".join(vf_filters)
            # M1/M2/M3 Mac 推荐用 videotoolbox 硬件加速编码，提升速度
            # 也可加 -threads 4 -preset veryfast 进一步加速
            cmd = [
                "ffmpeg", "-y",
                "-threads", "4",
                "-i", str(self.video_path),
                "-vf", vf_str,
                "-c:v", "h264_videotoolbox",
                "-b:v", "4M",
                "-preset", "veryfast",
                "-c:a", "copy",
                "-progress", "pipe:2",
                "-nostats",
                str(output_path)
            ]
            logger.info(f"[SyntheticVideo][{video_url}] 执行ffmpeg命令: {' '.join(cmd)}")
            # 获取视频总时长
            import subprocess as sp
            try:
                probe = sp.run([
                    "ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", str(self.video_path)
                ], capture_output=True, text=True, check=True)
                total_duration = float(probe.stdout.strip())
            except Exception:
                total_duration = None

            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            import time
            percent = 0
            last_sec = 0
            start_time = time.time()
            # ffmpeg -progress pipe:2 会把进度信息写到stderr
            cur_sec = 0
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                line = line.decode(errors='ignore').strip()
                if line.startswith('out_time_ms=') and total_duration:
                    ms = int(line.split('=')[1])
                    cur_sec = ms / 1e6
                    percent = min(100, int(cur_sec / total_duration * 100))
                    if int(cur_sec) != int(last_sec):
                        elapsed = time.time() - start_time
                        logger.info(f"[SyntheticVideo][{video_url}] ffmpeg进度: {percent}% ({cur_sec:.1f}s/{total_duration:.1f}s), 已耗时: {elapsed:.1f}s")
                        last_sec = cur_sec
                elif line.startswith('progress=') and line.split('=')[1] == 'end':
                    # 结束
                    break
            await proc.wait()
            if proc.returncode != 0:
                logger.error(f"[SyntheticVideo][{video_url}] ffmpeg合成失败: 进度{percent}%")
                raise RuntimeError(f"ffmpeg合成失败: 进度{percent}%")
            logger.info(f"[SyntheticVideo][{video_url}] 合成完成: {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"[SyntheticVideo][{video_url}] 合成失败: {str(e)}")
            raise

    def _generate_comments_ass(self, ass_path: Path):
        """
        生成ASS弹幕字幕，顶部，金黄色，描边黑色，点赞数和图标白色。
        自动检测视频分辨率同步 PlayResX/PlayResY。
        """
        fontname = self.find_chinese_font()
        # 自动检测视频分辨率
        try:
            import subprocess
            probe = subprocess.run([
                "ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height", "-of", "csv=p=0:s=x", str(self.video_path)
            ], capture_output=True, text=True, check=True)
            width, height = probe.stdout.strip().split('x')
        except Exception:
            width, height = '1920', '1080'
        ass_header = (
            f"[Script Info]\n"
            f"; Script generated by SyntheticVideo\n"
            f"Title: Comments\n"
            f"ScriptType: v4.00+\n"
            f"Collisions: Normal\n"
            f"PlayResX: {width}\n"
            f"PlayResY: {height}\n"
            f"WrapStyle: 2\n"
            f"ScaledBorderAndShadow: yes\n"
            f"YCbCr Matrix: TV.601\n\n"
            f"[V4+ Styles]\n"
            f"Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n"
            f"Style: UserName,{fontname},20,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,-1,0,0,0,100,100,0,0,1,2,0,8,40,40,10,1\n"
            f"Style: CommentText,{fontname},28,&H00FF9933,&H000000FF,&H00000000,&H00000000,-1,0,0,0,100,100,0,0,1,2,0,8,40,40,0,1\n"
            f"Style: LikeLine,{fontname},20,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,-1,0,0,0,100,100,0,0,1,2,0,8,40,40,0,1\n\n"
            f"[Events]\n"
            f"Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
        )
        # 只加载有点赞数的评论，按点赞数从高到低排序
        sorted_comments = sorted([c for c in self.comments if c.get("like_count", 0) > 0], key=lambda c: c.get("like_count", 0), reverse=True)
        ass_events = []
        def split_comment_lines(text, zh_len=20, en_len=40):
            import re
            lines = []
            buf = ''
            count = 0
            for ch in text:
                if '\u4e00' <= ch <= '\u9fff':
                    count += 2
                else:
                    count += 1
                buf += ch
                if (count >= zh_len*2) or (not ('\u4e00' <= ch <= '\u9fff') and count >= en_len):
                    lines.append(buf)
                    buf = ''
                    count = 0
            if buf:
                lines.append(buf)
            return '\\N'.join(line.strip() for line in lines if line.strip())

        for idx, c in enumerate(sorted_comments):
            # 每条评论从视频第24+idx*18秒出现，持续16秒，间隔2秒
            appear_time = 24 + idx * 18
            start = self._format_ass_time(appear_time)
            end = self._format_ass_time(appear_time + 16)
            text = c.get("translated_text") or c.get("text") or ""
            author = c.get("author", "")
            like_count = c.get("like_count", 0)
            # 自动分行，汉字每行20，英文每行40
            wrapped_text = split_comment_lines(text, zh_len=20, en_len=40)
            # 用户名在上，评论内容在中，点赞在下，分别用不同Style
            event_user = (
                f"Dialogue: 0,{start},{end},UserName,,0,0,0,,"
                f"{{\\b1}}{author}"
            )
            event_comment = (
                f"Dialogue: 0,{start},{end},CommentText,,0,0,0,,"
                f"{wrapped_text}"
            )
            event_like = (
                f"Dialogue: 0,{start},{end},LikeLine,,0,0,0,,"
                f"{{\\c&HFFFFFF&}}[赞]{like_count}"
            )
            ass_events.extend([event_user, event_comment, event_like])
        with open(ass_path, "w", encoding="utf-8") as f:
            f.write(ass_header)
            for e in ass_events:
                f.write(e + "\n")

    @staticmethod
    def _format_ass_time(seconds) -> str:
        """ASS时间格式化，自动处理str/int/float"""
        try:
            if isinstance(seconds, str):
                seconds = float(seconds.strip()) if seconds.strip() else 0.0
            elif not isinstance(seconds, (int, float)):
                seconds = 0.0
        except Exception:
            seconds = 0.0
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        cs = int((seconds - int(seconds)) * 100)
        return f"{h:d}:{m:02d}:{s:02d}.{cs:02d}"
