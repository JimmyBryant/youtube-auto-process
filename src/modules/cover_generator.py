
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
import logging
import os
from src.services.openai_service import OpenAIService


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
    font_size: int = 110,  # 入口参数不变，实际fit_font_size会动态更大
    outline_color: str = "white",
    outline_width: int = 4,
    margin_ratio: float = 0.08,
) -> Path:
    """
    使用OpenAI大模型根据标题生成封面文案，自动分行并每行不同颜色。
    """
    logger = logging.getLogger("cover_generator")
    try:
        if not title or not str(title).strip():
            raise ValueError("封面标题(title)不能为空！")
        # 1. 用OpenAI生成多行封面文案
        service = OpenAIService()
        cover_lines = service.generate_cover_title(title)
        logger.info(f"[cover_generator] AI生成封面文案: {cover_lines}")

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
            (253, 0, 5),    # 红 #ef4537
            (253, 237, 0),    # 黄 #fded00
            (30, 144, 255),   # 蓝
            (255, 105, 180),  # 粉
            (0, 206, 209),    # 青
        ]
        # 默认描边色为白色，黄色/深黄时用黑色
        def get_outline_color(main_color):
            # 判断是否为黄/深黄（色调在黄色区间，或RGB接近253,237,0等）
            yellow_like = [
                (253,237,0), (255,255,0), (255,215,0), (255,204,0), (255,223,34)
            ]
            for yc in yellow_like:
                if sum(abs(a-b) for a,b in zip(main_color,yc)) <= 60:
                    return (0,0,0)
            r,g,b = main_color
            if r > 180 and g > 140 and b < 80:
                return (0,0,0)
            return (255,255,255)
        max_width = int(W * 0.8)

        # 直接用AI返回的多行标题
        lines = [line for line in cover_lines if line.strip()][:3]

        # 动态调整字体大小（更大更粗，最大起始字号240，最小60），行数越少字号越大
        def fit_font_size(lines, font_path, max_width, max_height, min_size=60):
            # 行数越少，允许更大字号
            start_size = 240 if len(lines) == 1 else (180 if len(lines) == 2 else 140)
            size = start_size
            while size >= min_size:
                try:
                    font = ImageFont.truetype(font_path, size)
                except Exception:
                    font = ImageFont.load_default()
                total_height = len(lines) * size + (len(lines)-1)*int(size*0.2)
                too_wide = False
                for line in lines:
                    bbox = draw.textbbox((0, 0), line, font=font)
                    w = bbox[2] - bbox[0]
                    if w > max_width:
                        too_wide = True
                        break
                if total_height > max_height or too_wide:
                    size -= 4
                else:
                    return font, size
            try:
                font = ImageFont.truetype(font_path, min_size)
            except Exception:
                font = ImageFont.load_default()
            return font, min_size
        # 计算最大可用高度（上下边距）
        max_height = int(H * (1 - 2 * margin_ratio))
        font, font_size = fit_font_size(lines, font_path_used if font_path_used != 'PIL:default' else None, max_width, max_height)
        total_text_height = len(lines) * font_size + (len(lines)-1)*int(font_size*0.2)
        y = int(H * margin_ratio)
        y = max(y, (H - total_text_height)//2)
        for idx, line in enumerate(lines):
            bbox = draw.textbbox((0, 0), line, font=font)
            w = bbox[2] - bbox[0]
            h = font_size
            x = (W - w) // 2
            color = color_list[idx % len(color_list)]
            outline_color_this = get_outline_color(color)
            # 先描边
            for dx in range(-outline_width, outline_width+1):
                for dy in range(-outline_width, outline_width+1):
                    if dx != 0 or dy != 0:
                        draw.text((x+dx, y+dy), line, font=font, fill=outline_color_this)
            # 再主色
            draw.text((x, y), line, font=font, fill=color)
            y += font_size + int(font_size*0.2)
        img.save(output_path)
        logger.info(f"[cover_generator] 新封面已保存: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"[cover_generator] 生成封面失败: {e}")
        raise
