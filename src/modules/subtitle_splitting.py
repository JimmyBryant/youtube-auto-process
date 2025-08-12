from pathlib import Path
from typing import Union, List
import re
import math

def split_srt_file(input_path: Union[str, Path], output_path: Union[str, Path], max_line_length: int = 40) -> None:
    """
    读取SRT文件，对每条字幕内容按max_line_length切割，生成新的SRT文件（不修改原文件）。
    切割逻辑：优先按句末标点（。！？.!?…），再按逗号、分号、顿号等断句标点，最后按空格，实在不行硬切。
    """
    def split_text(text: str, max_len: int) -> List[str]:
        # 1. 先按句末标点分割
        end_punct = r'[。！？.!?…]'
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

        def further_split(sent: str, max_len: int) -> List[str]:
            sent = sent.strip()
            if len(sent) <= max_len:
                return [sent]
            # 2. 按逗号、分号、顿号等断句标点分割
            mid_punct = r'[，,；;、]'
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
            # 如果都不超长直接返回
            if all(len(s) <= max_len for s in mid_sents):
                return mid_sents
            # 3. 对超长的再按空格优雅切割
            result = []
            for s in mid_sents:
                if len(s) <= max_len:
                    result.append(s)
                else:
                    words = s.split(' ')
                    line = ''
                    for word in words:
                        if not line:
                            line = word
                        elif len(line) + 1 + len(word) <= max_len:
                            line += ' ' + word
                        else:
                            result.append(line)
                            line = word
                    if line:
                        result.append(line)
                    # 兜底：如果还有超长的，硬切
            final_result = []
            for l in result:
                if len(l) <= max_len:
                    final_result.append(l)
                else:
                    for i in range(0, len(l), max_len):
                        final_result.append(l[i:i+max_len])
            return final_result

        final = []
        for seg in sents:
            final.extend(further_split(seg, max_len))
        return final

    input_path = Path(input_path)
    output_path = Path(output_path)
    with input_path.open('r', encoding='utf-8') as f:
        lines = f.readlines()

    srt_blocks = []
    block = []
    for line in lines:
        if line.strip() == '':
            if block:
                srt_blocks.append(block)
                block = []
        else:
            block.append(line)
    if block:
        srt_blocks.append(block)

    new_blocks = []
    srt_idx = 1
    for blk in srt_blocks:
        # 解析时间轴和文本
        if len(blk) >= 3:
            time_line = blk[1].strip()
            text = ''.join(blk[2:]).strip().replace('\n', ' ').replace('\r', ' ')
        elif len(blk) >= 2:
            time_line = blk[1].strip()
            text = blk[-1].strip().replace('\n', ' ').replace('\r', ' ')
        else:
            time_line = ''
            text = blk[-1].strip() if blk else ''
        split_texts = split_text(text, max_line_length)
        n = len(split_texts)
        # 均分时间轴
        if time_line and '-->' in time_line and n > 0:
            try:
                t_start, t_end = [t.strip() for t in time_line.split('-->')]
                def parse_time(t: str) -> float:
                    # 支持 00:00:00,000
                    h, m, s_ms = t.split(':')
                    if ',' in s_ms:
                        s, ms = s_ms.split(',')
                        return int(h)*3600 + int(m)*60 + int(s) + int(ms)/1000
                    else:
                        return int(h)*3600 + int(m)*60 + float(s_ms)
                start_sec = parse_time(t_start)
                end_sec = parse_time(t_end)
                duration = (end_sec - start_sec) / n if n > 0 else 0
                def format_time(seconds: float) -> str:
                    hours = int(seconds // 3600)
                    minutes = int((seconds % 3600) // 60)
                    secs = int(seconds % 60)
                    ms = int(round((seconds - int(seconds)) * 1000))
                    return f"{hours:02}:{minutes:02}:{secs:02},{ms:03}"
                for i, sub_text in enumerate(split_texts):
                    sub_start = start_sec + i * duration
                    sub_end = start_sec + (i + 1) * duration if i < n - 1 else end_sec
                    new_blocks.append([
                        f"{srt_idx}\n",
                        f"{format_time(sub_start)} --> {format_time(sub_end)}\n",
                        f"{sub_text}\n",
                        '\n'
                    ])
                    srt_idx += 1
            except Exception:
                # 时间轴解析失败，原样输出
                new_blocks.append([
                    f"{srt_idx}\n",
                    blk[1] if len(blk) > 1 else '',
                    f"{text}\n",
                    '\n'
                ])
                srt_idx += 1
        else:
            # 没有时间轴，原样输出
            new_blocks.append([
                f"{srt_idx}\n",
                f"{text}\n",
                '\n'
            ])
            srt_idx += 1

    with output_path.open('w', encoding='utf-8') as f:
        for blk in new_blocks:
            for line in blk:
                f.write(line)



