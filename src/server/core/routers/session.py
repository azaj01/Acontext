from fastapi import APIRouter, Path
from fastapi.exceptions import HTTPException
from sqlalchemy import select, func, cast, Integer
from acontext_core.infra.db import DB_CLIENT
from acontext_core.env import LOG
from acontext_core.schema.api.response import Flag, LearningStatusResponse
from acontext_core.schema.utils import asUUID
from acontext_core.schema.orm import Task
from acontext_core.service.data import session as SD
from acontext_core.service.session_message import flush_session_message_blocking

router = APIRouter(prefix="/api/v1/project/{project_id}/session/{session_id}", tags=["session"])


@router.post("/flush")
async def session_flush(
    project_id: asUUID = Path(..., description="Project ID to search within"),
    session_id: asUUID = Path(..., description="Session ID to flush"),
) -> Flag:
    """
    Flush the session buffer for a given session.
    """
    LOG.info(f"Flushing session {session_id} for project {project_id}")
    r = await flush_session_message_blocking(project_id, session_id)
    return Flag(status=r.error.status.value, errmsg=r.error.errmsg)


@router.get("/get_learning_status")
async def get_learning_status(
    project_id: asUUID = Path(..., description="Project ID"),
    session_id: asUUID = Path(..., description="Session ID"),
) -> LearningStatusResponse:
    """
    Get learning status for a session.
    Returns the count of space digested tasks and not space digested tasks.
    If the session is not connected to a space, returns 0 and 0.
    """
    async with DB_CLIENT.get_session_context() as db_session:
        # Fetch the session to check if it's connected to a space
        r = await SD.fetch_session(db_session, session_id)
        if not r.ok():
            raise HTTPException(status_code=404, detail=str(r.error))

        session = r.data

        # If session is not connected to a space, return 0 and 0
        if session.space_id is None:
            return LearningStatusResponse(
                space_digested_count=0,
                not_space_digested_count=0,
            )

        # Get all tasks for this session and count space_digested status
        # Use cast to convert boolean to int for counting
        # For not_digested, use (1 - cast) to count False values
        query = (
            select(
                func.sum(cast(Task.space_digested, Integer)).label("digested_count"),
                func.sum(1 - cast(Task.space_digested, Integer)).label(
                    "not_digested_count"
                ),
            )
            .where(Task.session_id == session_id)
            .where(Task.is_planning == False)  # noqa: E712
            .where(Task.status == "success")  # only count successful tasks
        )

        result = await db_session.execute(query)
        row = result.first()

        if row is None:
            # No tasks found
            return LearningStatusResponse(
                space_digested_count=0,
                not_space_digested_count=0,
            )

        digested_count = int(row.digested_count or 0)
        not_digested_count = int(row.not_digested_count or 0)

        return LearningStatusResponse(
            space_digested_count=digested_count,
            not_space_digested_count=not_digested_count,
        )
