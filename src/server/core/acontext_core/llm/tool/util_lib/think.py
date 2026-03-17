from typing import Any
from ..base import Tool
from ....schema.llm import ToolSchema
from ....schema.result import Result
from ....env import LOG


async def _thinking_handler(
    ctx: Any,
    llm_arguments: dict,
) -> Result[str]:
    LOG.info(
        "agent.thinking",
        thinking=llm_arguments.get("thinking", "..."),
    )
    return Result.resolve("thinking reported")


_thinking_tool = (
    Tool()
    .use_schema(
        ToolSchema(
            function={
                "name": "report_thinking",
                "description": "Use this tool to report your thinking step by step. It will not obtain new information or change the database, but just append the thought to the log.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "thinking": {
                            "type": "string",
                            "description": "report your thinking here",
                        },
                    },
                    "required": ["thinking"],
                },
            }
        )
    )
    .use_handler(_thinking_handler)
)
