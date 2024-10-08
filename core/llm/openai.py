import json
from dataclasses import asdict, dataclass
from typing import Literal
from openai import AsyncOpenAI, OpenAI

from core.llm.exception import InvalidLLMJsonResponseError


@dataclass
class ChatMessage:
    role: Literal["system", "user", "assistant"]
    content: str


class OpenAIChatLLM:
    def __init__(
        self, 
        api_key: str, 
        model: str = "gpt-4o-mini", 
        temperature: float = 0.3,
        top_p: float = 0.2
        ):
        self.async_client = AsyncOpenAI(api_key=api_key)
        self.sync_client = OpenAI(api_key=api_key)

        self.model_kwargs = {"model": model, "temperature": temperature, "top_p": top_p}

    async def apredict_json(self, messages: list[ChatMessage]) -> dict:
        extra_params = {}
        if self.model_kwargs["model"] == "gpt-4o-mini":
            extra_params["response_format"] = {"type": "json_object"}

        resp = await self.async_client.chat.completions.create(
            messages=[asdict(message) for message in messages], **self.model_kwargs
        )
        resp_content = resp.choices[0].message.content
        resp_content = self.response_to_dict(text=resp_content)
        return resp_content

    def predict_json(self, messages: list[ChatMessage]) -> dict:
        extra_params = {}
        if self.model_kwargs["model"] == "gpt-4o-mini":
            extra_params["response_format"] = {"type": "json_object"}

        resp = self.sync_client.chat.completions.create(
            messages=[asdict(message) for message in messages], **self.model_kwargs
        )
        resp_content = resp.choices[0].message.content
        resp_content = self.response_to_dict(text=resp_content)
        return resp_content

    def response_to_dict(self, text: str) -> dict:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            raise InvalidLLMJsonResponseError(llm_response=text)