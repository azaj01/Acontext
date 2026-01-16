from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from ...schema.sandbox import (
    SandboxCreateConfig,
    SandboxUpdateConfig,
    SandboxRuntimeInfo,
    SandboxCommandOutput,
)
from ...schema.result import Result
from ...schema.orm import SandboxLog
from ...schema.utils import asUUID
from ...infra.sandbox.client import SANDBOX_CLIENT
from ...env import LOG


async def _get_sandbox_log(
    db_session: AsyncSession, sandbox_id: asUUID
) -> Result[SandboxLog]:
    """
    Get the SandboxLog record by unified sandbox ID.

    Args:
        db_session: Database session.
        sandbox_id: The unified sandbox ID (UUID).

    Returns:
        Result containing the SandboxLog record.
    """
    sandbox_log = await db_session.get(SandboxLog, sandbox_id)
    if sandbox_log is None:
        return Result.reject(f"Sandbox {sandbox_id} not found")
    return Result.resolve(sandbox_log)


async def create_sandbox(
    db_session: AsyncSession,
    project_id: asUUID,
    config: SandboxCreateConfig,
) -> Result[SandboxRuntimeInfo]:
    """
    Create and start a new sandbox, storing the ID mapping in the database.

    Args:
        db_session: Database session.
        project_id: The project ID to associate the sandbox with.
        config: Configuration for the sandbox including timeout, CPU, memory, etc.

    Returns:
        Result containing runtime information with the unified sandbox ID.
    """
    try:
        backend = SANDBOX_CLIENT.use_backend()

        # Create the sandbox in the backend
        info = await backend.start_sandbox(config)

        # Create the SandboxLog record to store the ID mapping
        sandbox_log = SandboxLog(
            project_id=project_id,
            backend_sandbox_id=info.sandbox_id,
            backend_type=backend.type,
            history_commands=[],
            generated_files=[],
        )
        db_session.add(sandbox_log)
        await db_session.flush()

        LOG.info(
            f"Created sandbox {sandbox_log.id} -> backend {backend.type}:{info.sandbox_id}"
        )

        # Replace the backend sandbox ID with the unified ID
        info.sandbox_id = str(sandbox_log.id)
        return Result.resolve(info)
    except ValueError as e:
        return Result.reject(f"Sandbox backend not available: {e}")
    except Exception as e:
        LOG.error(f"Failed to create sandbox: {e}")
        return Result.reject(f"Failed to create sandbox: {e}")


async def kill_sandbox(db_session: AsyncSession, sandbox_id: asUUID) -> Result[bool]:
    """
    Kill a running sandbox.

    Args:
        db_session: Database session.
        sandbox_id: The unified sandbox ID (UUID).

    Returns:
        Result containing True if the sandbox was killed successfully.
    """
    try:
        # Look up the backend sandbox ID
        log_result = await _get_sandbox_log(db_session, sandbox_id)
        if not log_result.ok():
            return Result.reject(log_result.error.errmsg)

        sandbox_log = log_result.data
        backend = SANDBOX_CLIENT.use_backend()
        success = await backend.kill_sandbox(sandbox_log.backend_sandbox_id)

        LOG.info(
            f"Killed sandbox {sandbox_id} (backend: {sandbox_log.backend_sandbox_id})"
        )
        return Result.resolve(success)
    except ValueError as e:
        return Result.reject(f"Sandbox backend not available: {e}")
    except Exception as e:
        LOG.error(f"Failed to kill sandbox {sandbox_id}: {e}")
        return Result.reject(f"Failed to kill sandbox: {e}")


async def get_sandbox(
    db_session: AsyncSession, sandbox_id: asUUID
) -> Result[SandboxRuntimeInfo]:
    """
    Get runtime information about a sandbox.

    Args:
        db_session: Database session.
        sandbox_id: The unified sandbox ID (UUID).

    Returns:
        Result containing runtime information about the sandbox.
    """
    try:
        # Look up the backend sandbox ID
        log_result = await _get_sandbox_log(db_session, sandbox_id)
        if not log_result.ok():
            return Result.reject(log_result.error.errmsg)

        sandbox_log = log_result.data
        backend = SANDBOX_CLIENT.use_backend()
        info = await backend.get_sandbox(sandbox_log.backend_sandbox_id)

        # Replace the backend sandbox ID with the unified ID
        info.sandbox_id = str(sandbox_id)
        return Result.resolve(info)
    except ValueError as e:
        return Result.reject(f"Sandbox not found or backend not available: {e}")
    except Exception as e:
        LOG.error(f"Failed to get sandbox {sandbox_id}: {e}")
        return Result.reject(f"Failed to get sandbox: {e}")


async def update_sandbox(
    db_session: AsyncSession,
    sandbox_id: asUUID,
    config: SandboxUpdateConfig,
) -> Result[SandboxRuntimeInfo]:
    """
    Update sandbox configuration (e.g., extend timeout).

    Args:
        db_session: Database session.
        sandbox_id: The unified sandbox ID (UUID).
        config: Update configuration (e.g., keepalive extension).

    Returns:
        Result containing runtime information about the updated sandbox.
    """
    try:
        # Look up the backend sandbox ID
        log_result = await _get_sandbox_log(db_session, sandbox_id)
        if not log_result.ok():
            return Result.reject(log_result.error.errmsg)

        sandbox_log = log_result.data
        backend = SANDBOX_CLIENT.use_backend()
        info = await backend.update_sandbox(sandbox_log.backend_sandbox_id, config)

        # Replace the backend sandbox ID with the unified ID
        info.sandbox_id = str(sandbox_id)
        return Result.resolve(info)
    except ValueError as e:
        return Result.reject(f"Sandbox not found or backend not available: {e}")
    except Exception as e:
        LOG.error(f"Failed to update sandbox {sandbox_id}: {e}")
        return Result.reject(f"Failed to update sandbox: {e}")


async def exec_command(
    db_session: AsyncSession,
    sandbox_id: asUUID,
    command: str,
) -> Result[SandboxCommandOutput]:
    """
    Execute a shell command in the sandbox.

    Args:
        db_session: Database session.
        sandbox_id: The unified sandbox ID (UUID).
        command: The shell command to execute.

    Returns:
        Result containing the command output (stdout, stderr, exit_code).
    """
    try:
        # Look up the backend sandbox ID
        log_result = await _get_sandbox_log(db_session, sandbox_id)
        if not log_result.ok():
            return Result.reject(log_result.error.errmsg)

        sandbox_log = log_result.data
        backend = SANDBOX_CLIENT.use_backend()
        output = await backend.exec_command(sandbox_log.backend_sandbox_id, command)

        # Update history_commands in the log
        sandbox_log.history_commands = [
            *sandbox_log.history_commands,
            {
                "command": command,
                "exit_code": output.exit_code,
            },
        ]
        await db_session.flush()

        return Result.resolve(output)
    except ValueError as e:
        return Result.reject(f"Sandbox not found or backend not available: {e}")
    except Exception as e:
        LOG.error(f"Failed to execute command in sandbox {sandbox_id}: {e}")
        return Result.reject(f"Failed to execute command: {e}")


async def download_file(
    db_session: AsyncSession,
    sandbox_id: asUUID,
    from_sandbox_file: str,
    download_to_s3_path: str,
) -> Result[bool]:
    """
    Download a file from the sandbox and upload it to S3.

    Args:
        db_session: Database session.
        sandbox_id: The unified sandbox ID (UUID).
        from_sandbox_file: The path to the file in the sandbox.
        download_to_s3_path: The S3 path to upload the file to.

    Returns:
        Result containing True if the file was transferred successfully.
    """
    try:
        # Look up the backend sandbox ID
        log_result = await _get_sandbox_log(db_session, sandbox_id)
        if not log_result.ok():
            return Result.reject(log_result.error.errmsg)

        sandbox_log = log_result.data
        backend = SANDBOX_CLIENT.use_backend()
        success = await backend.download_file(
            sandbox_log.backend_sandbox_id, from_sandbox_file, download_to_s3_path
        )

        if success:
            # Update generated_files in the log
            sandbox_log.generated_files = [
                *sandbox_log.generated_files,
                {
                    "sandbox_path": from_sandbox_file,
                    "s3_path": download_to_s3_path,
                },
            ]
            await db_session.flush()

        return Result.resolve(success)
    except ValueError as e:
        return Result.reject(f"Sandbox not found or backend not available: {e}")
    except Exception as e:
        LOG.error(f"Failed to download file from sandbox {sandbox_id}: {e}")
        return Result.reject(f"Failed to download file: {e}")


async def upload_file(
    db_session: AsyncSession,
    sandbox_id: asUUID,
    from_s3_file: str,
    upload_to_sandbox_path: str,
) -> Result[bool]:
    """
    Download a file from S3 and upload it to the sandbox.

    Args:
        db_session: Database session.
        sandbox_id: The unified sandbox ID (UUID).
        from_s3_file: The S3 path of the file to download.
        upload_to_sandbox_path: The parent directory in the sandbox to upload to.

    Returns:
        Result containing True if the file was transferred successfully.
    """
    try:
        # Look up the backend sandbox ID
        log_result = await _get_sandbox_log(db_session, sandbox_id)
        if not log_result.ok():
            return Result.reject(log_result.error.errmsg)

        sandbox_log = log_result.data
        backend = SANDBOX_CLIENT.use_backend()
        success = await backend.upload_file(
            sandbox_log.backend_sandbox_id, from_s3_file, upload_to_sandbox_path
        )
        return Result.resolve(success)
    except ValueError as e:
        return Result.reject(f"Sandbox not found or backend not available: {e}")
    except Exception as e:
        LOG.error(f"Failed to upload file to sandbox {sandbox_id}: {e}")
        return Result.reject(f"Failed to upload file: {e}")


async def get_sandbox_log(
    db_session: AsyncSession, sandbox_id: asUUID
) -> Result[SandboxLog]:
    """
    Get the SandboxLog record by unified sandbox ID.

    Args:
        db_session: Database session.
        sandbox_id: The unified sandbox ID (UUID).

    Returns:
        Result containing the SandboxLog record.
    """
    return await _get_sandbox_log(db_session, sandbox_id)


async def list_project_sandboxes(
    db_session: AsyncSession, project_id: asUUID
) -> Result[list[SandboxLog]]:
    """
    List all sandboxes for a project.

    Args:
        db_session: Database session.
        project_id: The project ID.

    Returns:
        Result containing a list of SandboxLog records.
    """
    try:
        query = select(SandboxLog).where(SandboxLog.project_id == project_id)
        result = await db_session.execute(query)
        sandbox_logs = list(result.scalars().all())
        return Result.resolve(sandbox_logs)
    except Exception as e:
        LOG.error(f"Failed to list sandboxes for project {project_id}: {e}")
        return Result.reject(f"Failed to list sandboxes: {e}")
