# 支持多LLM环境变量的翻译服务类
from pathlib import Path
import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

class TranslationService:
    def __init__(self, provider: str = None, target_lang: str = None):
        import os
        self.provider = provider or os.getenv("LLM_PROVIDER", "openai")
        self.target_lang = target_lang or os.getenv("TRANSLATE_TARGET_LANG", "zh")
        # 支持多家API KEY/BASE
        self.api_key = self._get_api_key(self.provider)
        self.api_base = self._get_api_base(self.provider)

    def _get_api_key(self, provider: str):
        env_map = {
            "openai": "OPENAI_API_KEY",
            "moonshot": "MOONSHOT_API_KEY",
            "baidu": "BAIDU_API_KEY",
        }
        key_env = env_map.get(provider.lower(), "LLM_API_KEY")
        return os.getenv(key_env)

    def _get_api_base(self, provider: str):
        env_map = {
            "openai": "OPENAI_API_BASE",
            "moonshot": "MOONSHOT_API_BASE",
            "baidu": "BAIDU_API_BASE",
        }
        default_map = {
            "openai": "https://api.openai.com/v1/chat/completions",
            "moonshot": "https://api.moonshot.cn/v1/chat/completions",
            "baidu": "https://qianfan.baidu.com/v1/chat/completions",
        }
        base_env = env_map.get(provider.lower(), "LLM_API_BASE")
        val = os.getenv(base_env)
        if val:
            return val
        return default_map.get(provider.lower())

    async def translate_subtitle(self, subtitle_path: Path) -> Path:
        """
        读取SRT字幕，自动分段调用大模型API翻译，输出翻译后SRT文件。
        :param subtitle_path: SRT字幕文件路径
        :return: 翻译后SRT文件路径
        """
        logger.info(f"Translating SRT: {subtitle_path}")
        # 读取SRT内容
        with open(subtitle_path, "r", encoding="utf-8") as f:
            srt_lines = f.readlines()

        # 解析SRT为块
        srt_blocks = []
        block = []
        for line in srt_lines:
            if line.strip() == "":
                if block:
                    srt_blocks.append(block)
                    block = []
            else:
                block.append(line)
        if block:
            srt_blocks.append(block)

        # 提取所有字幕文本行，带序号
        numbered_lines = []  # [(block_idx, line_idx, text)]
        for block_idx, blk in enumerate(srt_blocks):
            for line_idx, line in enumerate(blk[2:] if len(blk) >= 3 else blk[1:]):
                text = line.strip()
                if text:
                    numbered_lines.append((block_idx, line_idx, text))

        # 构造带序号的翻译输入
        prompt_lines = []
        for i, (_, _, text) in enumerate(numbered_lines):
            prompt_lines.append(f"{i+1}. {text}")
        prompt = "\n".join(prompt_lines)

        # 分段，避免API超长
        max_chars = 3500
        segments = []
        seg_indices = []
        current = []
        current_indices = []
        current_len = 0
        for idx, line in enumerate(prompt_lines):
            if current_len + len(line) > max_chars and current:
                segments.append(current)
                seg_indices.append(current_indices)
                current = [line]
                current_indices = [idx]
                current_len = len(line)
            else:
                current.append(line)
                current_indices.append(idx)
                current_len += len(line)
        if current:
            segments.append(current)
            seg_indices.append(current_indices)

        # 翻译每个分段，要求大模型按序号返回翻译
        translated_lines = [None] * len(numbered_lines)
        for idx_seg, (seg, seg_idx_list) in enumerate(zip(segments, seg_indices)):
            seg_prompt = "请将以下字幕内容逐行翻译为" + self.target_lang + "，严格按原序号返回翻译结果，每行格式为 '序号. 翻译内容'，不要解释：\n" + "\n".join(seg)
            logger.info(f"Translating segment {idx_seg+1}/{len(segments)} (lines: {len(seg)})...")
            logger.info("\n==== 段原文 ====")
            for l in seg:
                logger.info(l)
            try:
                translated = await translate_text_with_llm(
                    seg_prompt,
                    target_lang=self.target_lang,
                    provider=self.provider,
                    api_key=self.api_key,
                    api_base=self.api_base
                )
                logger.info(f"Segment {idx_seg+1} translation success.")
                logger.info("\n==== 段翻译结果 ====")
                for l in translated.strip().split("\n"):
                    logger.info(l)
                # 解析返回，提取序号和翻译内容
                for line in translated.strip().split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    if ". " in line:
                        num_str, content = line.split(". ", 1)
                    elif "." in line:
                        num_str, content = line.split(".", 1)
                        content = content.lstrip()
                    else:
                        continue
                    try:
                        idx_in_all = int(num_str) - 1
                        if 0 <= idx_in_all < len(translated_lines):
                            translated_lines[idx_in_all] = content
                    except Exception:
                        continue
            except Exception as e:
                logger.error(f"Segment {idx_seg+1} translation failed: {e}", exc_info=True)
                raise

        # 回填到SRT块
        # 先复制原块
        translated_blocks = [blk[:] for blk in srt_blocks]
        # 用翻译结果替换原文
        for i, (block_idx, line_idx, _) in enumerate(numbered_lines):
            fill_text = translated_lines[i] if translated_lines[i] is not None else ""
            blk = translated_blocks[block_idx]
            # 找到要替换的行（第3行及以后）
            if len(blk) >= 3:
                blk[2 + line_idx] = fill_text + "\n"
            elif len(blk) >= 1:
                blk[-1] = fill_text + "\n"

        # 写入新SRT
        out_path = subtitle_path.parent / f"{subtitle_path.stem}_{self.target_lang}_translated.srt"
        with open(out_path, "w", encoding="utf-8") as f:
            for blk in translated_blocks:
                for line in blk:
                    f.write(line)
                f.write("\n")
        logger.info(f"Translated SRT saved: {out_path}")
        return out_path
import logging
from pathlib import Path
from typing import Optional
import asyncio

logger = logging.getLogger('translation_service')


# 通用大语言模型API字幕翻译函数
import os
import aiohttp

async def translate_text_with_llm(text: str, target_lang: str = 'zh', provider: str = 'openai', api_key: str = None, api_base: str = None) -> str:
    import logging
    logger = logging.getLogger('translation_service')
    """
    使用大语言模型API翻译文本，支持多家API。
    :param text: 需要翻译的文本
    :param target_lang: 目标语言
    :param provider: LLM服务商(openai, moonshot, baidu等)
    :param api_key: API密钥
    :param api_base: API Base URL（如有）
    :return: 翻译后的文本
    """
    if not api_key:
        api_key = os.getenv("LLM_API_KEY")
    if not api_base:
        # 优先环境变量，其次知名LLM默认API_BASE
        default_map = {
            "openai": "https://api.openai.com/v1/chat/completions",
            "moonshot": "https://api.moonshot.cn/v1/chat/completions",
            "baidu": "https://qianfan.baidu.com/v1/chat/completions",
        }
        api_base = os.getenv("LLM_API_BASE") or default_map.get(provider, None)
    provider = provider or os.getenv("LLM_PROVIDER", "openai")
    target_lang = target_lang or os.getenv("TRANSLATE_TARGET_LANG", "zh")

    prompt = f"请将以下内容翻译成{target_lang}，只返回翻译后的文本，不要解释：\n{text}"

    # 代理支持
    proxy = os.getenv("HTTPS_PROXY") or os.getenv("ALL_PROXY") or os.getenv("HTTP_PROXY")
    session_args = {}
    if proxy:
        session_args["proxy"] = proxy

    logger.info(f"[LLM] Using api_base: {api_base}")
    logger.info(f"[LLM] Using proxy: {proxy}")

    if provider == "openai":
        url = api_base
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "gpt-3.5-turbo",
            "messages": [
                {"role": "system", "content": "You are a helpful translation assistant."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.2,
            "max_tokens": 2048
        }
        async with aiohttp.ClientSession(**session_args) as session:
            async with session.post(url, headers=headers, json=payload, timeout=60) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return data["choices"][0]["message"]["content"]
    elif provider == "moonshot":
        url = api_base
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "moonshot-v1-8k",
            "messages": [
                {"role": "system", "content": "You are a helpful translation assistant."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.2,
            "max_tokens": 2048
        }
        async with aiohttp.ClientSession(**session_args) as session:
            async with session.post(url, headers=headers, json=payload, timeout=60) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return data["choices"][0]["message"]["content"]
    elif provider == "baidu":
        url = api_base
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "ERNIE-Bot-8K",
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.2,
            "max_tokens": 2048
        }
        async with aiohttp.ClientSession(**session_args) as session:
            async with session.post(url, headers=headers, json=payload, timeout=60) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return data["result"] if "result" in data else data["choices"][0]["message"]["content"]
    else:
        raise NotImplementedError(f"Provider {provider} not supported yet.")