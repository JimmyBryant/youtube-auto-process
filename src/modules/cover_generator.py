
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
import logging
import os
from src.services.openai_service import OpenAIService
import random


"""
在原始封面图片上添加标题文本，生成新封面图片。
:param original_cover_path: 原始封面图片路径
:param title: 要写入的标题
"""
def generate_cover_with_ai(
    original_cover_path: Path,
    title: str,
    output_path: Path,
    font_path: str = None,
    font_size: int = 110,
    outline_color: str = "white",
    outline_width: int = 4,
    margin_ratio: float = 0.08,
) -> Path:
    """
    使用OpenAI大模型根据标题生成封面文案，自动分行并每行不同颜色。
    """
    logger = logging.getLogger("cover_generator")
    try:
        # 1. 用OpenAI生成封面文案
        service = OpenAIService()
        cover_text = service.generate_text(title)
        logger.info(f"[cover_generator] AI生成封面文案: {cover_text}")

        img = Image.open(original_cover_path).convert("RGBA")
        draw = ImageDraw.Draw(img)
        W, H = img.size
        # 自动查找字体（优先支持 emoji 的字体，其次粗体/黑体）
        font_candidates = []
        # 用户指定的 NotoSansSC-Medium.ttf 优先
        user_noto = "/Users/liuning/Library/Fonts/NotoSansSC-Medium.ttf"
        if os.path.exists(user_noto):
            font_candidates.append(user_noto)
        if font_path:
            font_candidates.append(font_path)
        import platform
        sys = platform.system()
        # 优先思源黑体（Noto Sans CJK SC/Noto Sans），各平台路径如下：
        noto_candidates = [
            # macOS
            "/System/Library/Fonts/Supplemental/NotoSansCJKsc-Bold.otf",
            "/System/Library/Fonts/Supplemental/NotoSansCJKsc-Regular.otf",
            # Linux
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
            # Windows（需用户自行安装）
            "C:/Windows/Fonts/NotoSansCJKsc-Bold.otf",
            "C:/Windows/Fonts/NotoSansCJKsc-Regular.otf",
        ]
        font_candidates += noto_candidates
        if sys == "Darwin":
            font_candidates += [
                "/System/Library/Fonts/Apple Color Emoji.ttc",  # emoji
                "/System/Library/Fonts/PingFang Bold.ttf",
                "/System/Library/Fonts/PingFang.ttc",
                "/System/Library/Fonts/Hiragino Sans GB W6.ttc",
                "/System/Library/Fonts/Hiragino Sans GB.ttc",
                "/System/Library/Fonts/STHeiti Bold.ttc",
                "/System/Library/Fonts/STHeiti Medium.ttc",
                "/System/Library/Fonts/Arial Unicode.ttf"
            ]
        elif sys == "Windows":
            font_candidates += [
                "C:/Windows/Fonts/seguiemj.ttf",  # emoji
                "C:/Windows/Fonts/msyhbd.ttc",
                "C:/Windows/Fonts/msyh.ttc",
                "C:/Windows/Fonts/simhei.ttf",
                "C:/Windows/Fonts/simsun.ttc"
            ]
        else:
            font_candidates += [
                "/usr/share/fonts/truetype/noto/NotoColorEmoji.ttf",
                "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
                "/usr/share/fonts/truetype/arphic/ukai.ttc"
            ]
        font = None
        font_path_used = None
        for fp in font_candidates:
            if os.path.exists(fp):
                try:
                    f = ImageFont.truetype(fp, font_size)
                    if font is None:
                        font = f
                        font_path_used = fp
                except Exception:
                    continue
        if font is None:
            font = ImageFont.load_default()
            font_path_used = 'PIL:default'
        logger.info(f"[cover_generator] 封面主字体: {font_path_used}")

        color_list = [
            (255, 48, 48),    # 红
            (255, 215, 0),    # 黄
            (30, 144, 255),   # 蓝
            (255, 105, 180),  # 粉
            (0, 206, 209),    # 青
        ]
        outline_color = (255, 255, 255)
        max_width = int(W * 0.92)
        # 分行逻辑：优先按标点符号切分，若单行过长再自动分行
        import re
        # 中文常用标点
        punc = r'[，。！？；,.!?;]'
        # 先按标点切分
        raw_lines = re.split(f'({punc})', cover_text)
        lines = []
        buf = ''
        for seg in raw_lines:
            buf += seg
            if re.match(punc, seg):
                lines.append(buf)
                buf = ''
        if buf.strip():
            lines.append(buf)
        # 再对每行做宽度限制，超长自动分行
        final_lines = []
        for line in lines:
            chars = list(line)
            while chars:
                for i in range(len(chars), 0, -1):
                    seg = ''.join(chars[:i])
                    bbox = draw.textbbox((0, 0), seg, font=font)
                    w = bbox[2] - bbox[0]
                    if w <= max_width:
                        final_lines.append(seg)
                        chars = chars[i:]
                        break
                else:
                    final_lines.append(chars[0])
                    chars = chars[1:]
        lines = [l for l in final_lines if l.strip()]
        total_text_height = len(lines) * font_size + (len(lines)-1)*int(font_size*0.2)
        y = int(H * margin_ratio)
        y = max(y, (H - total_text_height)//2)
        for idx, line in enumerate(lines):
            bbox = draw.textbbox((0, 0), line, font=font)
            w = bbox[2] - bbox[0]
            h = font_size
            x = (W - w) // 2
            color = color_list[idx % len(color_list)]
            # 先描边
            for dx in range(-outline_width, outline_width+1):
                for dy in range(-outline_width, outline_width+1):
                    if dx != 0 or dy != 0:
                        draw.text((x+dx, y+dy), line, font=font, fill=outline_color)
            # 再主色
            draw.text((x, y), line, font=font, fill=color)
            y += font_size + int(font_size*0.2)
        img.save(output_path)
        logger.info(f"[cover_generator] 新封面已保存: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"[cover_generator] 生成封面失败: {e}")
        raise
