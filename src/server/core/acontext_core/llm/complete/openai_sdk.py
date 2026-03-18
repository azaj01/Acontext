import json
from typing import Optional
from .clients import get_openai_async_client_instance
from openai.types.chat import ChatCompletion
from openai.types.chat import ChatCompletionMessageToolCall
from time import perf_counter
from ...env import LOG, DEFAULT_CORE_CONFIG
from ...schema.llm import LLMResponse
from ...telemetry.log import get_wide_event


def convert_openai_tool_to_llm_tool(tool_body: ChatCompletionMessageToolCall) -> dict:
    return {
        "id": tool_body.id,
        "type": tool_body.type,
        "function": {
            "name": tool_body.function.name,
            "arguments": json.loads(tool_body.function.arguments),
        },
    }


async def openai_complete(
    prompt=None,
    model=None,
    system_prompt=None,
    history_messages=[],
    json_mode=False,
    max_tokens=1024,
    prompt_kwargs: Optional[dict] = None,
    tools=None,
    **kwargs,
) -> LLMResponse:
    prompt_kwargs = prompt_kwargs or {}
    prompt_id = prompt_kwargs.get("prompt_id", "...")

    openai_async_client = get_openai_async_client_instance()

    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.extend(history_messages)
    if prompt:
        messages.append({"role": "user", "content": prompt})

    if not messages:
        raise ValueError("No messages provided")

    _start_s = perf_counter()
    response: ChatCompletion = await openai_async_client.chat.completions.create(
        model=model,
        messages=messages,
        timeout=DEFAULT_CORE_CONFIG.llm_response_timeout,
        max_tokens=max_tokens,
        tools=tools,
        **DEFAULT_CORE_CONFIG.llm_openai_completion_kwargs,
        **kwargs,
    )
    _end_s = perf_counter()
    _input = response.usage.prompt_tokens
    _output = response.usage.completion_tokens
    _cached = getattr(response.usage.prompt_tokens_details, "cached_tokens", None) or 0

    wide = get_wide_event()
    wide["llm_input_tokens"] = wide.get("llm_input_tokens", 0) + _input
    wide["llm_output_tokens"] = wide.get("llm_output_tokens", 0) + _output
    wide["llm_cached_tokens"] = wide.get("llm_cached_tokens", 0) + _cached

    LOG.info(
        "llm.complete",
        prompt_id=prompt_id,
        model=model,
        cached_tokens=_cached,
        input_tokens=_input,
        output_tokens=_output,
        total_tokens=_input + _output,
        duration_s=round(_end_s - _start_s, 4),
    )

    # Only support tool calls
    _tu = (
        [
            convert_openai_tool_to_llm_tool(tool)
            for tool in response.choices[0].message.tool_calls
        ]
        if response.choices[0].message.tool_calls
        else None
    )

    llm_response = LLMResponse(
        role=response.choices[0].message.role,
        raw_response=response,
        content=response.choices[0].message.content,
        tool_calls=_tu,
    )

    if json_mode:
        try:
            json_content = json.loads(response.choices[0].message.content)
        except json.JSONDecodeError:
            LOG.error(
                "llm.json_decode_error",
                content=response.choices[0].message.content[:200],
            )
            json_content = None
        llm_response.json_content = json_content

    return llm_response
