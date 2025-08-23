import os
import logging
from pathlib import Path
from typing import List, Dict, Any
import time

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
        优先级字体名称数组，遍历本地字体库所有字体文件名，返回第一个命中的字体名称。
        """
        import os
        # 字体名称优先级（适合字幕/ASS/ffmpeg）
        preferred_names = [
            "微软雅黑", "Microsoft YaHei",  # 优先匹配微软雅黑
            "NotoSansSC-Medium",  # 其次 NotoSansSC-Medium.ttf
            "Noto Sans CJK SC", "PingFang SC", "苹方", "Hiragino Sans GB",
            "黑体", "SimHei", "WenQuanYi Zen Hei", "STHeiti", "宋体", "SimSun", "STSong", "MSYH", "Arial"
        ]
        font_dirs = [
            os.path.expanduser("~/Library/Fonts"),
            "/System/Library/Fonts",
            "/Library/Fonts",
        ]
        # 收集本地所有字体文件名（不含扩展名）
        local_fonts = set()
        for font_dir in font_dirs:
            if os.path.isdir(font_dir):
                for fname in os.listdir(font_dir):
                    name, ext = os.path.splitext(fname)
                    if ext.lower() in [".ttf", ".ttc", ".otf"]:
                        local_fonts.add(name)
        # 优先级匹配
        for name in preferred_names:
            for local_name in local_fonts:
                if name.replace(" ","").lower() in local_name.replace(" ","").lower() or local_name.replace(" ","").lower() in name.replace(" ","").lower():
                    logger.info(f"[SyntheticVideo] 匹配到本地字体: {local_name} (优先名: {name})")
                    return local_name
        logger.warning("[SyntheticVideo] 未找到常见字体，将使用 Arial！")
        return 'Arial'
    @staticmethod
    def find_en_font():
        """
        优先级字体名称数组，优先匹配科技感、现代感的英文字体。
        """
        import os
        preferred_names = [
            "Orbitron", "Roboto", "Futura", "Arial", "Helvetica", "Segoe UI", "Verdana", "Tahoma"
        ]
        font_dirs = [
            os.path.expanduser("~/Library/Fonts"),
            "/System/Library/Fonts",
            "/Library/Fonts",
        ]
        local_fonts = set()
        for font_dir in font_dirs:
            if os.path.isdir(font_dir):
                for fname in os.listdir(font_dir):
                    name, ext = os.path.splitext(fname)
                    if ext.lower() in [".ttf", ".ttc", ".otf"]:
                        local_fonts.add(name)
        for name in preferred_names:
            for local_name in local_fonts:
                if name.replace(" ","").lower() in local_name.replace(" ","").lower() or local_name.replace(" ","").lower() in name.replace(" ","").lower():
                    logger.info(f"[SyntheticVideo] 匹配到英文字体: {local_name} (优先名: {name})")
                    return local_name
        logger.warning("[SyntheticVideo] 未找到科技感英文字体，将使用 Arial！")
        return 'Arial'
    async def synthesize(self) -> Path:
        """
        合成视频，先插入多字幕和带动画的评论，再拼接2秒封面头部。
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
            temp_content_video = self.output_dir / f"synthetic_content_{self.video_path.name}"

            # 检查是否有新封面图片
            cover_path = self.output_dir / "cover_with_title.png"
            has_cover = cover_path.exists()

            # 1. 生成评论弹幕ASS字幕文件（确保有评论才生成）
            ass_path = self.output_dir / f"comments.ass"
            if self.comments:
                self._generate_comments_ass(ass_path)
            # 2. 先合成内容视频（原视频+字幕+弹幕）

            zh_font = self.find_chinese_font()
            en_font = self.find_en_font()
            vf_filters = []
            vf_filters.append(
                f"subtitles='{self.orig_subtitle_path}':force_style='Fontname={en_font},Fontsize=20,PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,BorderStyle=1,Outline=1,Shadow=0,Alignment=2'"
            )
            vf_filters.append(
                f"subtitles='{self.zh_subtitle_path}':force_style='Fontname={zh_font},Fontsize=24,PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,BorderStyle=1,Outline=1,Shadow=0,Alignment=2,MarginV=30'"
            )
            if self.comments:
                vf_filters.append(f"ass={ass_path}")
            vf_str = ",".join(vf_filters)
            # 统一用libx264编码，保证拼接兼容性
            cmd_content = [
                "ffmpeg", "-y",
                "-threads", "4",
                "-i", str(self.video_path),
                "-vf", vf_str,
                "-c:v", "libx264",
                "-b:v", "4M",
                "-preset", "veryfast",
                "-c:a", "aac",
                str(temp_content_video)
            ]

            # 获取原视频总时长
            try:
                probe = subprocess.run([
                    "ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", str(self.video_path)
                ], capture_output=True, text=True, check=True)
                total_duration = float(probe.stdout.strip())
            except Exception:
                total_duration = 0.0
            logger.info(f"[SyntheticVideo][{video_url}] 合成内容视频: {' '.join(cmd_content)} (总时长: {total_duration:.1f}s)")
            t0 = time.time()
            proc = subprocess.Popen(cmd_content, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            last_log_time = 0
            current_time = 0.0
            while True:
                line = proc.stdout.readline()
                if not line:
                    break
                # 解析ffmpeg输出的time=字段
                if 'time=' in line:
                    import re
                    m = re.search(r'time=(\d+):(\d+):(\d+\.?\d*)', line)
                    if m:
                        h, m_, s = m.groups()
                        current_time = int(h)*3600 + int(m_)*60 + float(s)
                        elapsed = time.time() - t0
                        percent = (current_time/total_duration*100) if total_duration > 0 else 0
                        # 每1秒输出一次进度
                        if elapsed - last_log_time >= 1 or current_time >= total_duration:
                            logger.info(f"[SyntheticVideo][{video_url}] 进度: {current_time:.1f}/{total_duration:.1f}s ({percent:.1f}%)，已耗时 {elapsed:.1f}s")
                            last_log_time = elapsed
                if 'frame=' in line or 'speed=' in line:
                    logger.info(f"[ffmpeg] {line.strip()}")
            proc.wait()
            t1 = time.time()
            logger.info(f"[SyntheticVideo][{video_url}] 内容视频合成完成，耗时 {t1-t0:.1f} 秒，总时长 {total_duration:.1f}s")

            final_output = output_path
            if has_cover:
                # 3. 生成2秒封面视频（编码参数与内容视频一致，分辨率与内容视频一致）
                cover_video = self.output_dir / "cover_head.mp4"
                # 获取内容视频分辨率
                try:
                    probe = subprocess.run([
                        "ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height", "-of", "csv=p=0:s=x", str(self.video_path)
                    ], capture_output=True, text=True, check=True)
                    width, height = probe.stdout.strip().split('x')
                    width = int(width)
                    height = int(height)
                except Exception:
                    width, height = 1920, 1080
                scale_str = f"scale={width}:{height}"
                cmd_cover = [
                    "ffmpeg", "-y", "-loop", "1", "-i", str(cover_path),
                    "-f", "lavfi", "-t", "2", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
                    "-shortest", "-vf", scale_str, "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", str(cover_video)
                ]
                logger.info(f"[SyntheticVideo] 生成封面头部视频: {' '.join(map(str, cmd_cover))}")
                subprocess.run(cmd_cover, check=True)
                # 4. 拼接封面+内容视频（重新编码，保证兼容性）
                concat_list = self.output_dir / "concat_list.txt"
                with open(concat_list, "w") as f:
                    f.write(f"file '{cover_video}'\n")
                    f.write(f"file '{temp_content_video}'\n")
                concat_video = self.output_dir / "video_with_cover.mp4"
                cmd_concat = [
                    "ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(concat_list),
                    "-fflags", "+genpts",
                    "-c:v", "libx264", "-c:a", "aac", str(final_output)
                ]
                logger.info(f"[SyntheticVideo] 拼接封面与内容视频: {' '.join(map(str, cmd_concat))}")
                subprocess.run(cmd_concat, check=True)
            else:
                # 无封面，直接输出内容视频
                final_output = temp_content_video
            logger.info(f"[SyntheticVideo][{video_url}] 合成完成: {final_output}")
            return final_output
        except Exception as e:
            logger.error(f"[SyntheticVideo][{video_url}] 合成失败: {str(e)}")
            raise

    def _generate_comments_ass(self, ass_path: Path):
        """
        生成ASS弹幕字幕，顶部，金黄色，描边黑色，点赞数和图标白色。
        自动检测视频分辨率同步 PlayResX/PlayResY。
        支持通过 comment_style['comment_max_width_ratio'] 自定义最大宽度比例（默认0.8），自动推算每行最大字数。
        """
        fontname = self.find_chinese_font()
        # 只使用中文字体，不再使用 emoji 字体
        # 自动检测视频分辨率
        try:
            import subprocess
            probe = subprocess.run([
                "ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height", "-of", "csv=p=0:s=x", str(self.video_path)
            ], capture_output=True, text=True, check=True)
            width, height = probe.stdout.strip().split('x')
            width = int(width)
            height = int(height)
        except Exception:
            width, height = 1920, 1080
        # 计算最大宽度（默认80%）
        max_width_ratio = self.comment_style.get('comment_max_width_ratio', 0.8)
        max_width_px = int(width * max_width_ratio)
        # 估算每行最大汉字/英文字符数（假设字体宽度：中文约32px，英文约16px）
        zh_len = max(int(max_width_px / 32), 8)  # 最少8字
        en_len = max(int(max_width_px / 16), 16) # 最少16字
        # 允许通过comment_style自定义
        zh_len = self.comment_style.get('comment_zh_len', zh_len)
        en_len = self.comment_style.get('comment_en_len', en_len)
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
            f"Style: UserName,{fontname},32,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,-1,0,0,0,100,100,0,0,1,2,0,8,40,40,10,1\n"
            f"Style: CommentText,{fontname},48,&H00FF9933,&H000000FF,&H00000000,&H00000000,-1,0,0,0,100,100,0,0,1,2,0,8,40,40,30,1\n"
            f"Style: LikeLine,{fontname},32,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,-1,0,0,0,100,100,0,0,1,2,0,8,40,40,30,1\n\n"
            f"[Events]\n"
            f"Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
        )
        # 只加载有点赞数的评论，按点赞数从高到低排序
        sorted_comments = sorted([c for c in self.comments if c.get("like_count", 0) > 0], key=lambda c: c.get("like_count", 0), reverse=True)
        ass_events = []
        def split_comment_lines(text, zh_len=zh_len, en_len=en_len):
            """
            自动分行：汉字每行zh_len，英文每行en_len。宽度约为视频宽度的80%。
            可通过comment_style自定义。
            """
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
            return lines
        def ass_font_mixed(line):
            # 直接使用中文字体
            return line
        # 依次逐条显示评论弹幕，每条16秒
        base_time = 10.0
        duration = 16.0
        for idx, comment in enumerate(sorted_comments):
            author = comment.get("author", "")
            text = comment.get("translated_text", "")
            like_count = comment.get("like_count", 0)
            start_sec = base_time + idx * duration
            end_sec = start_sec + duration
            start = self._format_ass_time(start_sec)
            end = self._format_ass_time(end_sec)
            # 分行
            lines = split_comment_lines(text)
            ass_mixed = '\\N'.join([ass_font_mixed(line) for line in lines])
            # 用户名在上，评论内容在中，点赞在下，分别用不同Style
            event_user = (
                f"Dialogue: 0,{start},{end},UserName,,0,0,0,,"
                f"{{\\b1}}{author}"
            )
            event_comment = (
                f"Dialogue: 0,{start},{end},CommentText,,0,0,0,,"
                f"{ass_mixed}"
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
