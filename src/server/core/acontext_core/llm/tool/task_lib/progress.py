from ..base import Tool
from ....schema.llm import ToolSchema
from ....schema.result import Result
from ....service.data import task as TD
from ....schema.session.task import TaskStatus
from .ctx import TaskCtx


async def _append_task_progress_handler(
    ctx: TaskCtx,
    llm_arguments: dict,
) -> Result[str]:
    task_order: int = llm_arguments.get("task_order", None)
    progress: str = llm_arguments.get("progress", None)

    if task_order is None:
        return Result.resolve(
            "You must provide a task_order argument. Appending progress failed."
        )
    if task_order > len(ctx.task_ids_index) or task_order < 1:
        return Result.resolve(
            f"Task order {task_order} is out of range, appending progress failed."
        )
    if not progress or not progress.strip():
        return Result.resolve(
            "You must provide a non-empty progress string. Appending progress failed."
        )

    actually_task_id = ctx.task_ids_index[task_order - 1]
    actually_task = ctx.task_index[task_order - 1]

    if actually_task.status in (TaskStatus.SUCCESS, TaskStatus.FAILED):
        return Result.resolve(
            f"Appending progress failed. Task {task_order} is already {actually_task.status}. Update its status to 'running' first then append progress."
        )

    r = await TD.append_progress_to_task(
        ctx.db_session, actually_task_id, progress
    )
    if not r.ok():
        return r
    return Result.resolve(
        f"Progress appended to task {task_order}"
    )


_append_task_progress_tool = (
    Tool()
    .use_schema(
        ToolSchema(
            function={
                "name": "append_task_progress",
                "description": """Record a milestone-level progress summary for a task. Captures both agent progress and user-provided additional info.
- Agent progress: what the agent accomplished (e.g. code changes, actions taken).
- User additional info: clarifications, corrections, or supplementary context from the user. Prefix these with "User:" (e.g. "User: deploy target is AWS ECS").
- Aim for 1–3 entries per kind per task. Combine related steps into one entry. Keep it under ~150 characters.
- Be specific (file paths, values) but omit trivial details.
- Cannot append progress to 'success' or 'failed' tasks — update status to 'running' first.""",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "task_order": {
                            "type": "integer",
                            "description": "The order number of the task to append progress to.",
                        },
                        "progress": {
                            "type": "string",
                            "description": "One-sentence milestone summary (~150 chars max). For agent actions: 'Created login component in src/Login.tsx with form validation'. For user info: prefix with 'User:' e.g. 'User: database host is db.prod.internal, TLS required'.",
                        },
                    },
                    "required": ["task_order", "progress"],
                },
            }
        )
    )
    .use_handler(_append_task_progress_handler)
)
