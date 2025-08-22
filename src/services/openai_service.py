import os

import openai
from openai import OpenAI
import httpx
from dotenv import load_dotenv
load_dotenv()

class OpenAIService:
    """
    OpenAI GPT 服务封装，支持自定义模型、温度、最大长度等参数。
    """
    def __init__(self, api_key: str = None, model: str = "gpt-3.5-turbo"):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model
        # 代理支持
        proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy") or os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
        if proxy:
            # openai>=1.0.0 推荐用 transport 传递 httpx.Proxy
            transport = httpx.HTTPTransport(proxy=proxy)
            http_client = httpx.Client(transport=transport)
            self.client = OpenAI(api_key=self.api_key, http_client=http_client)
        else:
            self.client = OpenAI(api_key=self.api_key)

    def generate_text(self, title: str, temperature: float = 0.7, max_tokens: int = 128) -> str:
        """
        根据标题生成适合封面的文案。
        :param title: 视频标题
        :return: 适合封面的文案
        """
        prompt = (
            f"请根据以下YouTube视频标题，生成一句适合做视频封面的简洁有吸引力的中文文案（不超过20字），不要包含任何emoji或表情符号，只能用中文汉字、数字或标点：\n"
            f"标题：{title}"
        )
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
            max_tokens=max_tokens
        )
        return response.choices[0].message.content.strip()

# 用法示例：
# service = OpenAIService()
# result = service.generate_text("帮我写一句适合视频封面的文案，主题是AI学习")
# print(result)
