"""Capa de repositorios. Split entre dos backends:

- `repos.papers.*` → SingleStore (vector storage + fulltext search).
- `repos.jobs.*` y `repos.config.*` → Mongo (schema flexible, no necesita vectores).

Uso:
    from app import repos
    paper = await repos.papers.get("PAPER_1841")
    job   = await repos.jobs.get(job_id)
    cfg   = await repos.config.get()
"""
import json
from datetime import datetime, timezone
from typing import Any

from pymongo import ReturnDocument

from .db import get_mongo
from .singlestore import get_singlestore


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Papers — SingleStore
# ---------------------------------------------------------------------------


class _Papers:
    """CRUD sobre la tabla `papers` en SingleStore. Embedding se almacena como
    VECTOR(384) y se serializa desde/hacia listas Python via JSON_ARRAY_PACK."""

    # Columnas que se devuelven a la UI sin el embedding (peso muerto).
    _COLS_LIGHT = (
        "paper_id, title, abstract, year, "
        "score_keyword, score_tfidf, score_sbert, score_weighted, "
        "score_final, decision, justification, "
        "minio_key, tier_minio_key, created_at"
    )

    async def get(self, paper_id: str) -> dict[str, Any] | None:
        sql = f"SELECT {self._COLS_LIGHT} FROM papers WHERE paper_id = %s"
        row = await get_singlestore().fetchone(sql, (paper_id,))
        return self._normalize(row) if row else None

    async def get_with_embedding(self, paper_id: str) -> dict[str, Any] | None:
        sql = (
            f"SELECT {self._COLS_LIGHT}, JSON_ARRAY_UNPACK(embedding) AS embedding "
            "FROM papers WHERE paper_id = %s"
        )
        row = await get_singlestore().fetchone(sql, (paper_id,))
        return self._normalize(row) if row else None

    async def list(self, limit: int = 20, min_score: int = 0) -> list[dict[str, Any]]:
        sql = (
            f"SELECT {self._COLS_LIGHT} FROM papers "
            "WHERE score_final >= %s "
            "ORDER BY score_final DESC, score_weighted DESC LIMIT %s"
        )
        rows = await get_singlestore().fetchall(sql, (min_score, limit))
        return [self._normalize(r) for r in rows]

    async def ranking(self, top: int = 10) -> list[dict[str, Any]]:
        sql = (
            f"SELECT {self._COLS_LIGHT} FROM papers "
            "ORDER BY score_weighted DESC, score_final DESC LIMIT %s"
        )
        rows = await get_singlestore().fetchall(sql, (top,))
        return [self._normalize(r) for r in rows]

    async def insert(self, doc: dict[str, Any]) -> None:
        await self.insert_bulk([doc])

    async def insert_bulk(self, docs: list[dict[str, Any]]) -> None:
        """Bulk INSERT. Genera VALUES (...), (...) en una sola sentencia.
        Embeddings se serializan como JSON arrays y se empaquetan con JSON_ARRAY_PACK."""
        if not docs:
            return
        chunks: list[str] = []
        params: list[Any] = []
        for d in docs:
            chunks.append(
                "(%s,%s,%s,%s,%s,JSON_ARRAY_PACK(%s),%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            )
            params.extend([
                d["paper_id"],
                d.get("title", ""),
                d.get("abstract", ""),
                d.get("text_full") or d.get("text") or "",
                d.get("year") or 2024,
                json.dumps(d["embedding"]) if d.get("embedding") else "[]",
                float(d.get("score_keyword") or 0.0),
                float(d.get("score_tfidf") or 0.0),
                float(d.get("score_sbert") or 0.0),
                float(d.get("score_weighted") or 0.0),
                int(d.get("score_final") or 0),
                d.get("decision", ""),
                d.get("minio_key"),
                d.get("tier_minio_key"),
                d.get("created_at") or _now_iso(),
            ])
        sql = (
            "INSERT INTO papers (paper_id, title, abstract, text_full, year, embedding, "
            "score_keyword, score_tfidf, score_sbert, score_weighted, "
            "score_final, decision, minio_key, tier_minio_key, created_at) VALUES "
            + ",".join(chunks)
        )
        await get_singlestore().execute(sql, tuple(params))

    async def update(self, paper_id: str, fields: dict[str, Any]) -> None:
        """Update parcial. NO acepta embedding (usar update_embedding)."""
        if not fields:
            return
        sets: list[str] = []
        params: list[Any] = []
        for k, v in fields.items():
            if k == "embedding":
                continue
            sets.append(f"{k} = %s")
            params.append(v)
        if not sets:
            return
        params.append(paper_id)
        sql = f"UPDATE papers SET {','.join(sets)} WHERE paper_id = %s"
        await get_singlestore().execute(sql, tuple(params))

    async def update_embedding(self, paper_id: str, embedding: list[float]) -> None:
        await get_singlestore().execute(
            "UPDATE papers SET embedding = JSON_ARRAY_PACK(%s) WHERE paper_id = %s",
            (json.dumps(embedding), paper_id),
        )

    async def list_for_tier_sync(self) -> list[dict[str, Any]]:
        """Trae lo mínimo para sincronizar tier folders tras reclassify."""
        return await get_singlestore().fetchall(
            "SELECT paper_id, score_final, minio_key, tier_minio_key FROM papers"
        )

    async def count(self) -> int:
        row = await get_singlestore().fetchone("SELECT COUNT(*) AS n FROM papers")
        return int(row["n"]) if row else 0

    async def count_by_tier(self) -> dict[str, int]:
        """Buckets gold/silver/bronze/out_of_range para el dashboard."""
        rows = await get_singlestore().fetchall(
            "SELECT score_final, COUNT(*) AS n FROM papers GROUP BY score_final"
        )
        buckets = {"gold": 0, "silver": 0, "bronze": 0, "out_of_range": 0}
        for r in rows:
            s = int(r["score_final"] or 0)
            if s == 0:
                buckets["out_of_range"] += int(r["n"])
            elif s >= 4:
                buckets["gold"] += int(r["n"])
            elif s == 3:
                buckets["silver"] += int(r["n"])
            else:
                buckets["bronze"] += int(r["n"])
        return buckets

    async def max_paper_n(self) -> int:
        """Mayor sufijo numérico entre paper_ids existentes (para init del counter)."""
        try:
            row = await get_singlestore().fetchone(
                "SELECT MAX(CAST(SUBSTRING(paper_id, 7) AS UNSIGNED)) AS m FROM papers "
                "WHERE paper_id RLIKE '^PAPER_[0-9]+$'"
            )
        except Exception:
            return 2000
        return int(row["m"]) if row and row.get("m") else 2000

    @staticmethod
    def _normalize(row: dict[str, Any]) -> dict[str, Any]:
        out = dict(row)
        if out.get("created_at") is not None and not isinstance(out["created_at"], str):
            out["created_at"] = (
                out["created_at"].isoformat() if hasattr(out["created_at"], "isoformat") else str(out["created_at"])
            )
        if "embedding" in out and out["embedding"] is not None:
            v = out["embedding"]
            try:
                if isinstance(v, (bytes, bytearray)):
                    out["embedding"] = json.loads(v.decode() if isinstance(v, bytes) else v)
                elif isinstance(v, str):
                    out["embedding"] = json.loads(v)
            except Exception:
                out["embedding"] = None
        # score_breakdown como dict (compat con el shape original).
        out["score_breakdown"] = {
            "keyword": float(out.get("score_keyword") or 0),
            "tfidf": float(out.get("score_tfidf") or 0),
            "sbert": float(out.get("score_sbert") or 0),
            "weighted": float(out.get("score_weighted") or 0),
        }
        return out


# ---------------------------------------------------------------------------
# Jobs — Mongo
# ---------------------------------------------------------------------------


class _Jobs:
    async def insert(self, job: dict[str, Any]) -> None:
        await get_mongo().jobs.insert_one(job)

    async def update(self, job_id: str, fields: dict[str, Any]) -> dict[str, Any] | None:
        fields = {**fields, "updated_at": _now_iso()}
        return await get_mongo().jobs.find_one_and_update(
            {"job_id": job_id},
            {"$set": fields},
            projection={"_id": 0},
            return_document=ReturnDocument.AFTER,
        )

    async def get(self, job_id: str) -> dict[str, Any] | None:
        return await get_mongo().jobs.find_one({"job_id": job_id}, projection={"_id": 0})

    async def list(self, limit: int = 50) -> list[dict[str, Any]]:
        cursor = get_mongo().jobs.find({}, projection={"_id": 0}).sort("created_at", -1).limit(limit)
        return await cursor.to_list(limit)

    async def count_in_flight(self) -> int:
        return await get_mongo().jobs.count_documents({"status": {"$ne": "completed"}})


# ---------------------------------------------------------------------------
# Config — Mongo (singleton doc en collection `config`)
# ---------------------------------------------------------------------------


class _Config:
    async def get(self) -> dict[str, Any] | None:
        return await get_mongo().config.find_one({"_id": "current"})

    async def update(self, fields: dict[str, Any]) -> dict[str, Any] | None:
        fields = {**fields, "updated_at": _now_iso()}
        return await get_mongo().config.find_one_and_update(
            {"_id": "current"},
            {"$set": fields},
            return_document=ReturnDocument.AFTER,
        )


# ---------------------------------------------------------------------------
# Counter atómico para paper_id — Mongo
# ---------------------------------------------------------------------------


async def next_paper_id() -> str:
    counter = await get_mongo().counters.find_one_and_update(
        {"_id": "paper_id"},
        {"$inc": {"seq": 1}},
        return_document=ReturnDocument.AFTER,
    )
    if counter is None:
        max_n = await papers.max_paper_n()
        await get_mongo().counters.update_one(
            {"_id": "paper_id"},
            {"$set": {"seq": max_n + 1}},
            upsert=True,
        )
        return f"PAPER_{max_n + 1}"
    return f"PAPER_{counter['seq']}"


async def init_counters() -> None:
    """Idempotente. Sincroniza el counter con el max de papers en SingleStore."""
    max_n = await papers.max_paper_n()
    existing = await get_mongo().counters.find_one({"_id": "paper_id"})
    if not existing:
        await get_mongo().counters.insert_one({"_id": "paper_id", "seq": max_n})
    elif existing.get("seq", 0) < max_n:
        await get_mongo().counters.update_one({"_id": "paper_id"}, {"$set": {"seq": max_n}})


# Singletons exportados
papers = _Papers()
jobs = _Jobs()
config = _Config()
