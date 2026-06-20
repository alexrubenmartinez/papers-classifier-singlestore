"""papers-api — FastAPI con storage split (SingleStore + Mongo + MinIO).

Estructura:
- Papers (con embeddings VECTOR(384)) → SingleStore (repos.papers).
- Jobs + config → Mongo (repos.jobs, repos.config).
- PDFs originales + tier folders → MinIO.

Decisiones de performance:
- _run_import_batch usa ProcessPoolExecutor para paralelizar PyMuPDF + SBERT + scoring.
  WORKERS = cpu_count - 1. Bulk INSERT a SingleStore en lotes de 100.
- _run_reclassify ejecuta UNA sola sentencia UPDATE SQL con DOT_PRODUCT y MATCH AGAINST.
  Solo se cae a Python para sincronizar tier folders en MinIO (server-side copy).
"""
import asyncio
import json
import os
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, File, HTTPException, Path, Query, Request, UploadFile
from minio.commonconfig import CopySource
from sse_starlette.sse import EventSourceResponse

from app import pipeline, repos
from app.db import (
    MINIO_BUCKET,
    MINIO_PREFIX,
    copy_to_tier,
    ensure_bucket,
    get_minio,
    get_pdf,
    init_indices,
    list_objects,
    put_pdf,
    remove_object,
    seed_config_if_empty,
    tier_for_score,
    tier_key,
)
from app.schemas import (
    ChatRequest,
    ChatResponse,
    ImportRequest,
    ImportResponse,
    JobStatus,
    JustifyBatchResponse,
    JustifyRequest,
    JustifyResponse,
    Paper,
    PaperSummary,
    QueryConfig,
    QueryConfigUpdate,
    ReclassifyResponse,
    UploadResponse,
)
from app.singlestore import get_singlestore, ping as ping_singlestore

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
OLLAMA_DEFAULT_MODEL = os.getenv("OLLAMA_DEFAULT_MODEL", "qwen2.5:1.5b")
WORKERS = max(1, (os.cpu_count() or 4) - 1)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@asynccontextmanager
async def lifespan(_: FastAPI):
    ensure_bucket()
    await init_indices()
    pipeline.get_model()
    print("[startup] SBERT model loaded")
    if await ping_singlestore():
        print("[startup] SingleStore reachable")
    else:
        print("[startup] WARNING: SingleStore no responde — verifica SINGLESTORE_URL")
    if await seed_config_if_empty(pipeline.embed_text):
        print("[startup] seeded config")
    await repos.init_counters()
    n_papers = await repos.papers.count()
    print(f"[startup] papers en SingleStore: {n_papers}")
    print(f"[startup] WORKERS para import paralelo: {WORKERS}")
    yield


app = FastAPI(
    title="papers-api",
    version="1.0.0",
    description=(
        "Pipeline de clasificación con SingleStore como vector store. "
        "Reclassify ejecuta una sola sentencia SQL con DOT_PRODUCT y MATCH AGAINST."
    ),
    lifespan=lifespan,
)


@app.get("/")
async def root() -> dict[str, str]:
    return {
        "service": "papers-api",
        "storage": "singlestore + mongo + minio",
        "docs": "/docs",
        "openapi": "/openapi.json",
    }


@app.get("/health")
async def health() -> dict:
    n_papers = await repos.papers.count()
    by_tier = await repos.papers.count_by_tier()
    return {
        "status": "ok",
        "service": "papers-api",
        "version": "1.0.0",
        "papers": n_papers,
        "papers_by_tier": by_tier,
        "jobs_in_flight": await repos.jobs.count_in_flight(),
        "sbert_loaded": pipeline._model is not None,
        "singlestore_reachable": await ping_singlestore(),
        "workers": WORKERS,
    }


@app.post("/papers", response_model=UploadResponse, status_code=202)
async def upload_paper(
    file: UploadFile = File(...),
    justify: str = Query("none", regex="^(none|lazy|auto)$"),
) -> UploadResponse:
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Solo se aceptan archivos .pdf")
    data = await file.read()
    if not data:
        raise HTTPException(400, "Archivo vacío")

    paper_id = await repos.next_paper_id()
    job_id = str(uuid.uuid4())
    minio_key = put_pdf(paper_id, data)

    job = {
        "job_id": job_id,
        "paper_id": paper_id,
        "type": "ingest",
        "status": "queued",
        "stage": "queued",
        "filename": file.filename,
        "size_bytes": len(data),
        "minio_key": minio_key,
        "justify_mode": justify,
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "error": None,
    }
    await repos.jobs.insert(job)
    asyncio.create_task(_run_pipeline_real(job_id, paper_id, minio_key, justify_mode=justify))

    return UploadResponse(
        job_id=job_id,
        paper_id=paper_id,
        status="queued",
        stream_url=f"/stream?job_id={job_id}",
    )


@app.get("/papers", response_model=list[PaperSummary])
async def list_papers_endpoint(
    limit: int = Query(20, ge=1, le=5000),
    min_score: int = Query(0, ge=0, le=5),
) -> list[dict]:
    return await repos.papers.list(limit=limit, min_score=min_score)


@app.get("/papers/{paper_id}", response_model=Paper)
async def get_paper_endpoint(paper_id: str = Path(...)) -> dict:
    paper = await repos.papers.get(paper_id)
    if not paper:
        raise HTTPException(404, f"Paper {paper_id} no encontrado")
    return paper


@app.get("/ranking", response_model=list[PaperSummary])
async def ranking_endpoint(top: int = Query(10, ge=1, le=50)) -> list[dict]:
    return await repos.papers.ranking(top=top)


@app.get("/jobs", response_model=list[JobStatus])
async def list_jobs_endpoint(limit: int = Query(50, ge=1, le=500)) -> list[dict]:
    return await repos.jobs.list(limit=limit)


@app.get("/jobs/{job_id}", response_model=JobStatus)
async def get_job_endpoint(job_id: str = Path(...)) -> dict:
    job = await repos.jobs.get(job_id)
    if not job:
        raise HTTPException(404, f"Job {job_id} no encontrado")
    return job


@app.get("/config", response_model=QueryConfig)
async def get_config_endpoint() -> dict:
    cfg = await repos.config.get()
    if not cfg:
        raise HTTPException(500, "Config no inicializada")
    cfg.pop("query_embedding", None)
    cfg.pop("_id", None)
    return cfg


@app.put("/config", response_model=QueryConfig)
async def update_config_endpoint(
    body: QueryConfigUpdate,
    reclassify: bool = Query(False),
) -> dict:
    update: dict = {k: v for k, v in body.model_dump().items() if v is not None}
    if not update:
        raise HTTPException(400, "Body vacío")
    if "query_text" in update:
        update["query_embedding"] = pipeline.embed_text(update["query_text"])
    cfg = await repos.config.update(update)
    if not cfg:
        raise HTTPException(500, "Config no inicializada")
    if reclassify:
        asyncio.create_task(_run_reclassify(str(uuid.uuid4()), is_auto=True))
    cfg.pop("query_embedding", None)
    cfg.pop("_id", None)
    return cfg


# ---------------------------------------------------------------------------
# Reclassify — SQL puro en SingleStore (la operación que más se beneficia)
# ---------------------------------------------------------------------------


@app.post("/reclassify", response_model=ReclassifyResponse, status_code=202)
async def reclassify_endpoint(
    justify: str = Query("none", regex="^(none|gold_only|all)$"),
) -> ReclassifyResponse:
    total = await repos.papers.count()
    if total == 0:
        raise HTTPException(400, "No hay papers para reclasificar")
    job_id = str(uuid.uuid4())
    job = {
        "job_id": job_id,
        "paper_id": "ALL",
        "type": "reclassify_sql",
        "status": "queued",
        "stage": "queued",
        "filename": None,
        "size_bytes": 0,
        "minio_key": None,
        "total": total,
        "processed": 0,
        "justify_mode": justify,
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "error": None,
    }
    await repos.jobs.insert(job)
    asyncio.create_task(_run_reclassify(job_id, justify_mode=justify))
    return ReclassifyResponse(
        job_id=job_id,
        type="reclassify_all",
        total=total,
        stream_url=f"/stream?job_id={job_id}",
    )


async def _run_reclassify(job_id: str, justify_mode: str = "none", is_auto: bool = False) -> None:
    """Reclassify ultra-rápido: UNA sentencia UPDATE en SingleStore + tier sync MinIO."""
    try:
        start = time.monotonic()
        cfg = await repos.config.get()
        if not cfg:
            raise RuntimeError("Config no inicializada")

        qemb = cfg.get("query_embedding") or []
        kw_str = " ".join(
            kw for axis_kws in (cfg.get("axes") or {}).values() for kw in axis_kws
        ) or cfg.get("query_text", "")
        w = cfg.get("weights") or {"keyword": 0.40, "tfidf": 0.30, "sbert": 0.30}
        t = cfg.get("thresholds") or {
            "gold_muy": 0.65, "gold_claro": 0.50, "revisar": 0.35, "no_prioritario": 0.20,
        }
        ymin, ymax = cfg.get("year_range") or [2016, 2026]

        await repos.jobs.update(job_id, {"status": "running", "stage": "scoring_sql"})

        sql = """
            UPDATE papers SET
              score_sbert    = (DOT_PRODUCT(embedding, JSON_ARRAY_PACK(%s)) + 1) / 2,
              score_tfidf    = LEAST(1.0, COALESCE(MATCH(text_full) AGAINST (%s), 0) / 10),
              score_weighted = COALESCE(score_keyword, 0) * %s
                             + COALESCE(score_tfidf, 0)  * %s
                             + COALESCE(score_sbert, 0)  * %s,
              score_final = CASE
                WHEN year < %s OR year > %s THEN 0
                WHEN score_weighted >= %s THEN 5
                WHEN score_weighted >= %s THEN 4
                WHEN score_weighted >= %s THEN 3
                WHEN score_weighted >= %s THEN 2
                ELSE 1 END,
              decision = CASE
                WHEN year < %s OR year > %s THEN 'Fuera del rango temporal'
                WHEN score_weighted >= %s THEN 'Gold — muy relacionado'
                WHEN score_weighted >= %s THEN 'Gold — claramente relacionado'
                WHEN score_weighted >= %s THEN 'Revisar (parcial)'
                WHEN score_weighted >= %s THEN 'No prioritario'
                ELSE 'Excluido' END
        """
        params = (
            json.dumps(qemb), kw_str,
            w["keyword"], w["tfidf"], w["sbert"],
            ymin, ymax,
            t["gold_muy"], t["gold_claro"], t["revisar"], t["no_prioritario"],
            ymin, ymax,
            t["gold_muy"], t["gold_claro"], t["revisar"], t["no_prioritario"],
        )
        await get_singlestore().execute(sql, params)
        sql_elapsed = time.monotonic() - start

        await repos.jobs.update(job_id, {"stage": "tier_sync", "elapsed_seconds": int(sql_elapsed)})
        rows = await repos.papers.list_for_tier_sync()
        total = len(rows)
        moves = 0
        for i, row in enumerate(rows, start=1):
            mk = row.get("minio_key")
            if not mk:
                continue
            expected = tier_key(row["paper_id"], int(row.get("score_final") or 0))
            actual = row.get("tier_minio_key")
            if expected != actual:
                try:
                    copy_to_tier(row["paper_id"], mk, int(row.get("score_final") or 0))
                    if actual and actual != expected:
                        remove_object(actual)
                    await repos.papers.update(row["paper_id"], {"tier_minio_key": expected})
                    moves += 1
                except Exception as exc:
                    print(f"[tier sync] {row['paper_id']} falló: {exc}")
            if i % 200 == 0:
                await repos.jobs.update(job_id, {"stage": f"tier_sync {i}/{total} (moves={moves})"})

        total_elapsed = int(time.monotonic() - start)
        await repos.jobs.update(job_id, {
            "status": "completed",
            "stage": f"done — sql={int(sql_elapsed)}s tier_moves={moves}",
            "elapsed_seconds": total_elapsed,
            "processed": total,
            "total": total,
        })
    except Exception as exc:
        await repos.jobs.update(job_id, {"status": "failed", "error": f"{type(exc).__name__}: {exc}"})


# ---------------------------------------------------------------------------
# Import batch desde MinIO — paralelizado con ProcessPoolExecutor
# ---------------------------------------------------------------------------


@app.post("/papers/import", response_model=ImportResponse, status_code=202)
async def import_papers_endpoint(body: ImportRequest) -> ImportResponse:
    try:
        keys = list_objects(body.source_prefix)
    except Exception as exc:
        raise HTTPException(500, f"No se pudo listar el prefix: {exc}") from exc
    if not keys:
        raise HTTPException(404, f"No se encontraron PDFs bajo '{body.source_prefix}'")
    if body.limit:
        keys = keys[: body.limit]

    job_id = str(uuid.uuid4())
    total = len(keys)
    job = {
        "job_id": job_id,
        "paper_id": "ALL",
        "type": "import_batch",
        "status": "queued",
        "stage": f"0/{total}",
        "filename": None,
        "size_bytes": 0,
        "minio_key": None,
        "total": total,
        "processed": 0,
        "source_prefix": body.source_prefix,
        "justify_mode": body.justify,
        "workers": WORKERS,
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "error": None,
    }
    await repos.jobs.insert(job)
    asyncio.create_task(_run_import_batch(job_id, keys, body.justify))
    return ImportResponse(
        job_id=job_id,
        type="import_batch",
        total=total,
        stream_url=f"/stream?job_id={job_id}",
    )


async def _run_import_batch(job_id: str, source_keys: list[str], justify_mode: str) -> None:
    """Import paralelizado. PyMuPDF + SBERT + scoring corren en workers (CPU-bound).
    El orquestador hace I/O (MinIO + Mongo + SingleStore writes)."""
    minio_client = get_minio()
    total = len(source_keys)
    start = time.monotonic()
    chunk_size = 50
    bulk_buffer: list[dict] = []
    BULK_FLUSH_AT = 100

    try:
        cfg = await repos.config.get()
        if not cfg:
            raise RuntimeError("Config no inicializada")
        cfg_serializable = {
            "axes": cfg.get("axes") or {},
            "query_text": cfg.get("query_text") or "",
            "query_embedding": cfg.get("query_embedding") or [],
            "weights": cfg.get("weights") or {"keyword": 0.40, "tfidf": 0.30, "sbert": 0.30},
            "year_range": cfg.get("year_range") or [2016, 2026],
            "thresholds": cfg.get("thresholds") or {
                "gold_muy": 0.65, "gold_claro": 0.50, "revisar": 0.35, "no_prioritario": 0.20,
            },
        }

        loop = asyncio.get_running_loop()
        with ProcessPoolExecutor(max_workers=WORKERS) as pool:
            for offset in range(0, total, chunk_size):
                chunk_keys = source_keys[offset : offset + chunk_size]
                chunk_ids = [await repos.next_paper_id() for _ in chunk_keys]

                dst_keys: list[str] = []
                for src, pid in zip(chunk_keys, chunk_ids):
                    dst = f"{MINIO_PREFIX}/{pid}.pdf"
                    minio_client.copy_object(MINIO_BUCKET, dst, CopySource(MINIO_BUCKET, src))
                    dst_keys.append(dst)

                pdf_bytes_list = [get_pdf(dst) for dst in dst_keys]

                futures = [
                    loop.run_in_executor(pool, pipeline.process_one, b, cfg_serializable)
                    for b in pdf_bytes_list
                ]
                try:
                    results = await asyncio.gather(*futures)
                except Exception as exc:
                    print(f"[import] chunk falló: {exc}")
                    results = [None] * len(futures)

                for pid, dst_key, res in zip(chunk_ids, dst_keys, results):
                    if res is None:
                        continue
                    try:
                        tier_dst = copy_to_tier(pid, dst_key, res["score_final"])
                    except Exception as exc:
                        print(f"[tier copy] {pid} falló: {exc}")
                        tier_dst = None
                    bulk_buffer.append({
                        **res,
                        "paper_id": pid,
                        "minio_key": dst_key,
                        "tier_minio_key": tier_dst,
                        "created_at": _now_iso(),
                    })

                if len(bulk_buffer) >= BULK_FLUSH_AT:
                    await repos.papers.insert_bulk(bulk_buffer)
                    bulk_buffer.clear()

                processed = min(offset + chunk_size, total)
                elapsed = time.monotonic() - start
                rate = processed / elapsed if elapsed > 0 else 0
                eta = (total - processed) / rate if rate > 0 else 0
                await repos.jobs.update(job_id, {
                    "stage": f"{processed}/{total} · ETA {int(eta)}s",
                    "status": "running",
                    "processed": processed,
                    "total": total,
                    "elapsed_seconds": int(elapsed),
                    "seconds_per_paper": round(elapsed / processed, 3) if processed else 0,
                    "rate_papers_per_sec": round(rate, 2),
                })

        if bulk_buffer:
            await repos.papers.insert_bulk(bulk_buffer)
            bulk_buffer.clear()

        total_elapsed = int(time.monotonic() - start)
        await repos.jobs.update(job_id, {
            "status": "completed",
            "stage": f"{total}/{total} — done en {total_elapsed}s",
            "elapsed_seconds": total_elapsed,
            "processed": total,
        })
    except Exception as exc:
        await repos.jobs.update(job_id, {"status": "failed", "error": f"{type(exc).__name__}: {exc}"})


async def _run_pipeline_real(job_id: str, paper_id: str, minio_key: str, justify_mode: str = "none") -> None:
    """Ingest de UN paper (upload desde UI). Sin ProcessPool — overhead > beneficio para 1 paper."""
    try:
        cfg = await repos.config.get()
        if not cfg:
            raise RuntimeError("Config no inicializada")

        cfg_serializable = {
            "axes": cfg.get("axes") or {},
            "query_text": cfg.get("query_text") or "",
            "query_embedding": cfg.get("query_embedding") or [],
            "weights": cfg.get("weights") or {"keyword": 0.40, "tfidf": 0.30, "sbert": 0.30},
            "year_range": cfg.get("year_range") or [2016, 2026],
            "thresholds": cfg.get("thresholds") or {
                "gold_muy": 0.65, "gold_claro": 0.50, "revisar": 0.35, "no_prioritario": 0.20,
            },
        }

        await repos.jobs.update(job_id, {"status": "running", "stage": "processing"})
        pdf_bytes = get_pdf(minio_key)
        result = await asyncio.to_thread(pipeline.process_one, pdf_bytes, cfg_serializable)

        await repos.jobs.update(job_id, {"stage": "tier_copy"})
        try:
            tier_dst = copy_to_tier(paper_id, minio_key, result["score_final"])
        except Exception as exc:
            print(f"[tier copy] {paper_id} falló: {exc}")
            tier_dst = None

        await repos.papers.insert({
            **result,
            "paper_id": paper_id,
            "minio_key": minio_key,
            "tier_minio_key": tier_dst,
            "created_at": _now_iso(),
        })

        if justify_mode == "auto":
            await repos.jobs.update(job_id, {"stage": "ollama_justify"})
            try:
                fresh = await repos.papers.get(paper_id)
                if fresh:
                    text, _ms = await _justify_paper(fresh)
                    await repos.papers.update(paper_id, {"justification": text})
            except Exception as exc:
                print(f"[justify auto] {paper_id} falló: {exc}")

        await repos.jobs.update(job_id, {"status": "completed", "stage": "completed"})
    except Exception as exc:
        await repos.jobs.update(job_id, {"status": "failed", "error": f"{type(exc).__name__}: {exc}"})


# ---------------------------------------------------------------------------
# Justify (Ollama)
# ---------------------------------------------------------------------------


async def _justify_paper(paper: dict) -> tuple[str, int]:
    cfg = await repos.config.get()
    topic = (cfg or {}).get("topic_name", "el tema activo")
    sb = paper.get("score_breakdown") or {}
    prompt_system = (
        "Sos un asistente experto en revisión bibliográfica. Responde SIEMPRE en español neutro, "
        "en una o dos oraciones, justificando la decisión asignada a un paper académico."
    )
    prompt_user = (
        f"Tema activo: {topic}\n"
        f"Paper:\n  Título: {paper.get('title', '')}\n"
        f"  Abstract: {(paper.get('abstract') or '(sin abstract)')[:1200]}\n"
        f"Scoring:\n"
        f"  - keyword: {sb.get('keyword', 0):.2f}\n"
        f"  - tfidf:   {sb.get('tfidf', 0):.2f}\n"
        f"  - sbert:   {sb.get('sbert', 0):.2f}\n"
        f"  - weighted: {sb.get('weighted', 0):.2f}\n"
        f"Decisión: {paper.get('decision', '?')} (score {paper.get('score_final', '?')}/5).\n"
        f"Justifica brevemente por qué esa decisión tiene sentido."
    )
    payload = {
        "model": OLLAMA_DEFAULT_MODEL,
        "stream": False,
        "messages": [
            {"role": "system", "content": prompt_system},
            {"role": "user", "content": prompt_user},
        ],
    }
    async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as client:
        try:
            r = await client.post(f"{OLLAMA_URL}/api/chat", json=payload)
        except httpx.RequestError as exc:
            raise HTTPException(502, f"Ollama unreachable: {exc}") from exc
    if r.status_code != 200:
        raise HTTPException(502, f"Ollama error {r.status_code}: {r.text[:200]}")
    data = r.json()
    text = (data.get("message", {}).get("content") or "").strip()
    ms = int(data.get("total_duration", 0) / 1_000_000)
    return text, ms


@app.post("/justify/{paper_id}", response_model=JustifyResponse)
async def justify_paper_endpoint(paper_id: str = Path(...)) -> JustifyResponse:
    paper = await repos.papers.get(paper_id)
    if not paper:
        raise HTTPException(404, f"Paper {paper_id} no encontrado")
    text, ms = await _justify_paper(paper)
    await repos.papers.update(paper_id, {"justification": text})
    return JustifyResponse(paper_id=paper_id, justification=text, generated_in_ms=ms)


@app.post("/justify", response_model=JustifyBatchResponse, status_code=202)
async def justify_batch_endpoint(body: JustifyRequest) -> JustifyBatchResponse:
    if body.paper_ids:
        paper_ids = list(body.paper_ids)
    elif body.top is not None:
        rank = await repos.papers.ranking(top=body.top)
        paper_ids = [p["paper_id"] for p in rank]
    elif body.min_score is not None:
        rows = await repos.papers.list(limit=5000, min_score=body.min_score)
        paper_ids = [p["paper_id"] for p in rows]
    else:
        raise HTTPException(400, "Body vacío: pasar paper_ids, top o min_score")
    if not paper_ids:
        raise HTTPException(400, "No hay papers que matcheen el filtro")

    job_id = str(uuid.uuid4())
    total = len(paper_ids)
    job = {
        "job_id": job_id,
        "paper_id": "ALL",
        "type": "justify_batch",
        "status": "queued",
        "stage": f"0/{total}",
        "total": total,
        "processed": 0,
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "error": None,
    }
    await repos.jobs.insert(job)
    asyncio.create_task(_run_justify_batch(job_id, paper_ids))
    return JustifyBatchResponse(
        job_id=job_id,
        type="justify_batch",
        total=total,
        stream_url=f"/stream?job_id={job_id}",
    )


async def _run_justify_batch(job_id: str, paper_ids: list[str]) -> None:
    total = len(paper_ids)
    try:
        for i, pid in enumerate(paper_ids, start=1):
            paper = await repos.papers.get(pid)
            if not paper:
                continue
            try:
                text, _ms = await _justify_paper(paper)
                await repos.papers.update(pid, {"justification": text})
            except Exception as exc:
                print(f"[justify_batch] {pid} falló: {exc}")
            await repos.jobs.update(job_id, {
                "stage": f"{i}/{total}",
                "status": "running",
                "processed": i,
                "total": total,
            })
        await repos.jobs.update(job_id, {"status": "completed", "stage": f"{total}/{total}"})
    except Exception as exc:
        await repos.jobs.update(job_id, {"status": "failed", "error": f"{type(exc).__name__}: {exc}"})


# ---------------------------------------------------------------------------
# SSE + chat
# ---------------------------------------------------------------------------


@app.get("/stream")
async def stream(request: Request, job_id: str | None = Query(None)) -> EventSourceResponse:
    async def event_generator():
        last_stage: str | None = None
        while True:
            if await request.is_disconnected():
                break
            if job_id:
                job = await repos.jobs.get(job_id)
                if not job:
                    yield {"event": "error", "data": json.dumps({"detail": "job not found"})}
                    break
                if job.get("stage") != last_stage:
                    last_stage = job.get("stage")
                    yield {"event": "job.progress", "data": json.dumps(job, default=str)}
                if job.get("status") in ("completed", "failed"):
                    yield {"event": f"job.{job['status']}", "data": json.dumps(job, default=str)}
                    break
            else:
                yield {
                    "event": "heartbeat",
                    "data": json.dumps({
                        "time": _now_iso(),
                        "jobs_in_flight": await repos.jobs.count_in_flight(),
                    }),
                }
            await asyncio.sleep(1)

    return EventSourceResponse(event_generator())


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest) -> ChatResponse:
    model = req.model or OLLAMA_DEFAULT_MODEL
    payload = {
        "model": model,
        "stream": False,
        "messages": [m.model_dump() for m in req.messages],
    }
    async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as client:
        try:
            r = await client.post(f"{OLLAMA_URL}/api/chat", json=payload)
        except httpx.RequestError as exc:
            raise HTTPException(502, f"Ollama unreachable: {exc}") from exc
    if r.status_code != 200:
        raise HTTPException(502, f"Ollama upstream error {r.status_code}: {r.text[:200]}")
    data = r.json()
    return ChatResponse(
        model=model,
        message=data.get("message", {}).get("content", ""),
        total_duration_ms=int(data.get("total_duration", 0) / 1_000_000),
        eval_count=data.get("eval_count", 0),
    )
