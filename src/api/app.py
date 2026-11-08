import asyncio
import logging
import uuid
import shutil
from contextlib import asynccontextmanager, suppress
from time import monotonic

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi_pagination import Page, add_pagination
from fastapi_pagination.ext.sqlalchemy import apaginate
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from api import schemas
from api.dispatch import maintenance_loop
from api.uploads import filename_from_header, save_upload, disk
from core.broker import broker
from core.config import MAX_UPLOADS, TARGET_ROOT
from core.database import async_session_maker, engine
from core.heartbeat import TaskHeartbeat
from core.models import WorkItem
from core.runtime import TERMINAL, finish_task, finish_upload
from core.tables import CaseTable, EvidenceTable, TaskTable

log = logging.getLogger(__name__)
upload_slots = asyncio.Semaphore(MAX_UPLOADS)


@asynccontextmanager
async def lifespan(_):
    maintenance = asyncio.create_task(maintenance_loop(), name="task-maintenance")
    try:
        yield
    finally:
        maintenance.cancel()
        with suppress(asyncio.CancelledError):
            await maintenance
        await broker.shutdown()
        await engine.dispose()


app = FastAPI(title="GFlow Parser", version="0.2.0", lifespan=lifespan)


async def locked_case(session, cid):
    case = await session.scalar(select(CaseTable).where(CaseTable.id == cid).with_for_update())
    if case is None:
        raise HTTPException(404, "Case not found")
    return case


async def read_evidence(eid) -> schemas.EvidenceRead:
    async with async_session_maker() as session:
        evidence = await session.get(EvidenceTable, eid)
        if evidence is None:
            raise HTTPException(404, "Evidence not found")
        return schemas.EvidenceRead.model_validate(evidence)


@app.get("/health")
async def health():
    async with async_session_maker() as session:
        await session.execute(select(1))
    return {"status": "ok"}


@app.get("/cases", response_model=Page[schemas.CaseRead])
async def list_cases():
    async with async_session_maker() as session:
        return await apaginate(session, select(CaseTable).order_by(CaseTable.id.desc()))


@app.post("/cases", response_model=schemas.CaseRead, status_code=201)
async def create_case(body: schemas.CaseCreate):
    try:
        async with async_session_maker.begin() as session:
            case = CaseTable(**body.model_dump())
            session.add(case)
            await session.flush()
            result = schemas.CaseRead.model_validate(case)
    except IntegrityError:
        raise HTTPException(409, "Case short_name already exists") from None
    return result


@app.delete("/cases/{cid}", status_code=204)
async def delete_case(cid: int):
    async with async_session_maker.begin() as session:
        case = await locked_case(session, cid)
        active = await session.scalar(
            select(EvidenceTable.id)
            .where(EvidenceTable.case_id == cid, EvidenceTable.status.not_in(TERMINAL))
            .limit(1)
        )
        if active is not None:
            raise HTTPException(409, "Case has active evidence")
        await session.delete(case)
        await disk(shutil.rmtree, f'{TARGET_ROOT}/{case.short_name}', ignore_errors=True)
    return Response(status_code=204)


@app.get("/cases/{cid}/evidences", response_model=Page[schemas.EvidenceRead])
async def list_evidences(cid: int):
    async with async_session_maker() as session:
        if await session.get(CaseTable, cid) is None:
            raise HTTPException(404, "Case not found")
        return await apaginate(
            session,
            select(EvidenceTable).where(EvidenceTable.case_id == cid).order_by(EvidenceTable.id.desc()),
        )


@app.post("/cases/{cid}/evidences", response_model=schemas.EvidenceRead, status_code=202)
async def upload_evidence(cid: int, request: Request):
    filename = filename_from_header(request.headers.get("filename"))
    async with upload_slots:
        token = str(uuid.uuid4())
        try:
            async with async_session_maker.begin() as session:
                case = await locked_case(session, cid)
                evidence = EvidenceTable(
                    case_id=cid,
                    uid=f"{case.short_name}-{token}",
                    filename=filename,
                    storage_path=f"{case.short_name}/{token}/{filename}",
                )
                session.add(evidence)
                await session.flush()
                task = TaskTable(
                    evidence_id=evidence.id,
                    name="upload",
                    status="running",
                    heartbeat_at=await session.scalar(select(func.now())),
                )
                session.add(task)
                await session.flush()
                eid, task_id = evidence.id, task.id
                item = WorkItem(task_id, eid, "upload", "", evidence.uid, evidence.storage_path)
        except IntegrityError:
            raise HTTPException(409, "Filename already belongs to an Evidence in this Case") from None
        state = TaskHeartbeat(item, asyncio.current_task())
        state.start()
        try:
            size, checksum = await save_upload(request, state)
            if not await finish_upload(task_id, size, checksum, monotonic() - state.started):
                raise HTTPException(409, "Upload heartbeat expired")
        except BaseException as exc:
            if isinstance(exc, asyncio.CancelledError):
                # Persist a terminal upload failure on disconnect/shutdown/heartbeat loss.
                asyncio.current_task().uncancel()
            message = state.failure or (str(exc) or type(exc).__name__)
            try:
                await finish_task(task_id, None, message, monotonic() - state.started)
            except Exception:
                log.exception("Unable to save upload failure task_id=%s", task_id)
            if isinstance(exc, (HTTPException, asyncio.CancelledError)):
                raise
            log.exception("Upload failed evidence_id=%s", eid)
            raise HTTPException(500, "Upload failed; consult Evidence status and service logs") from exc
        finally:
            await state.stop()
    return await read_evidence(eid)


@app.get("/evidences/{eid}", response_model=schemas.EvidenceRead)
async def get_evidence(eid: int):
    return await read_evidence(eid)


async def locked_evidence(session, eid):
    cid = await session.scalar(select(EvidenceTable.case_id).where(EvidenceTable.id == eid))
    if cid is None:
        raise HTTPException(404, "Evidence not found")
    case = await locked_case(session, cid)
    evidence = await session.scalar(select(EvidenceTable).where(EvidenceTable.id == eid).with_for_update())
    if evidence is None:
        raise HTTPException(404, "Evidence not found")
    if evidence.status not in TERMINAL:
        raise HTTPException(409, "Evidence is still active")
    return case, evidence


@app.delete("/evidences/{eid}", status_code=204)
async def delete_evidence(eid: int):
    async with async_session_maker.begin() as session:
        _, evidence = await locked_evidence(session, eid)
        await session.delete(evidence)
        await disk(shutil.rmtree, f'{TARGET_ROOT}/{evidence.storage_path}', ignore_errors=True)
    return Response(status_code=204)


add_pagination(app)
