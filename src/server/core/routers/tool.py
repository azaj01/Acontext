from typing import List
from fastapi import APIRouter, Path, Body
from fastapi.exceptions import HTTPException
from acontext_core.infra.db import DB_CLIENT
from acontext_core.schema.api.request import ToolRenameRequest
from acontext_core.schema.api.response import Flag
from acontext_core.schema.tool.tool_reference import ToolReferenceData
from acontext_core.schema.utils import asUUID
from acontext_core.service.data import tool as TT

router = APIRouter(prefix="/api/v1/project/{project_id}/tool", tags=["tool"])


@router.post("/rename")
async def project_tool_rename(
    project_id: asUUID = Path(..., description="Project ID to rename tool within"),
    request: ToolRenameRequest = Body(..., description="Request to rename tool"),
) -> Flag:
    rename_list = [(t.old_name.strip(), t.new_name.strip()) for t in request.rename]
    async with DB_CLIENT.get_session_context() as db_session:
        r = await TT.rename_tool(db_session, project_id, rename_list)
    return Flag(status=r.error.status.value, errmsg=r.error.errmsg)


@router.get("/name")
async def get_project_tool_names(
    project_id: asUUID = Path(..., description="Project ID to get tool names within"),
) -> List[ToolReferenceData]:
    async with DB_CLIENT.get_session_context() as db_session:
        r = await TT.get_tool_names(db_session, project_id)
        if not r.ok():
            raise HTTPException(status_code=500, detail=r.error)
    return r.data
