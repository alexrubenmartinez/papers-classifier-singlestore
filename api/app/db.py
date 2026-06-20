"""Mongo (jobs + config + counters) + MinIO (PDFs + tier folders).

Papers viven en SingleStore (ver app/singlestore.py). Aquí solo lo que va a Mongo
o MinIO.
"""
import io
import os
from datetime import datetime, timezone

from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:REPLACE_PASSWORD@mongodb:27017/?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "papers_api")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "REPLACE_PASSWORD")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "examen-parcial")
MINIO_PREFIX = os.getenv("MINIO_PREFIX", "papers-api/uploads")
MINIO_TIER_PREFIX = os.getenv("MINIO_TIER_PREFIX", "papers-api")

_mongo_client: AsyncIOMotorClient | None = None
_minio_client: Minio | None = None


def get_mongo() -> AsyncIOMotorDatabase:
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = AsyncIOMotorClient(MONGO_URI)
    return _mongo_client[MONGO_DB]


def get_minio() -> Minio:
    global _minio_client
    if _minio_client is None:
        _minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
    return _minio_client


async def init_indices() -> None:
    """Solo crea indices para jobs + counters. Los indices de papers viven en
    SingleStore (ver infra/singlestore/schema.sql)."""
    db = get_mongo()
    await db.jobs.create_index("job_id", unique=True)
    await db.jobs.create_index("created_at")


def ensure_bucket() -> None:
    client = get_minio()
    try:
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
    except S3Error:
        pass


def put_pdf(paper_id: str, data: bytes, content_type: str = "application/pdf") -> str:
    client = get_minio()
    key = f"{MINIO_PREFIX}/{paper_id}.pdf"
    client.put_object(
        MINIO_BUCKET,
        key,
        io.BytesIO(data),
        length=len(data),
        content_type=content_type,
    )
    return key


def get_pdf(minio_key: str) -> bytes:
    client = get_minio()
    resp = client.get_object(MINIO_BUCKET, minio_key)
    try:
        return resp.read()
    finally:
        resp.close()
        resp.release_conn()


def tier_for_score(score: int) -> str:
    """Mapea score (0-5) a nombre del folder del tier."""
    if score == 0:
        return "out_of_range"
    if score >= 4:
        return "gold"
    if score == 3:
        return "silver"
    return "bronze"


def tier_key(paper_id: str, score: int) -> str:
    """Devuelve la key MinIO en la que vive (o vivirá) el PDF para ese tier."""
    return f"{MINIO_TIER_PREFIX}/{tier_for_score(score)}/{paper_id}.pdf"


def copy_to_tier(paper_id: str, src_key: str, score: int) -> str:
    """Copia server-side al folder del tier. Sobreescribe si existe."""
    client = get_minio()
    dst = tier_key(paper_id, score)
    client.copy_object(
        MINIO_BUCKET,
        dst,
        CopySource(MINIO_BUCKET, src_key),
    )
    return dst


def remove_object(key: str) -> None:
    """Idempotente."""
    client = get_minio()
    try:
        client.remove_object(MINIO_BUCKET, key)
    except S3Error:
        pass


def list_objects(prefix: str) -> list[str]:
    """Lista keys PDF bajo un prefijo (recursivo). Util para import batch."""
    client = get_minio()
    return [
        obj.object_name
        for obj in client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)
        if obj.object_name and obj.object_name.lower().endswith(".pdf")
    ]


# ---------------------------------------------------------------------------
# Config seed (Mongo)
# ---------------------------------------------------------------------------

SEED_CONFIG: dict = {
    "_id": "current",
    "topic_name": "Zero Trust + IA en Ciberseguridad",
    "query_text": (
        "Zero trust architecture for cybersecurity threat detection using "
        "artificial intelligence machine learning in cloud and hybrid environments. "
        "Includes NIST 800-207, ZTNA, microsegmentation, intrusion detection, "
        "anomaly detection, and AI-powered security."
    ),
    "axes": {
        "keywords": [
            "zero trust", "zero-trust", "ztna", "beyondcorp", "nist 800-207",
            "microsegmentation", "cybersecurity", "cyber security",
            "cloud security", "threat detection", "intrusion detection",
            "anomaly detection", "artificial intelligence", "machine learning",
            "deep learning", "ai-powered", "ai-driven",
        ],
    },
    "weights": {"keyword": 0.40, "tfidf": 0.30, "sbert": 0.30},
    "year_range": [2016, 2026],
    "thresholds": {
        "gold_muy": 0.65,
        "gold_claro": 0.50,
        "revisar": 0.35,
        "no_prioritario": 0.20,
    },
    "query_embedding": None,
    "updated_at": None,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def seed_config_if_empty(embed_fn) -> bool:
    """Seed del config inicial. embed_fn recibe el query_text y devuelve el embedding."""
    db = get_mongo()
    if await db.config.count_documents({}) == 0:
        doc = dict(SEED_CONFIG)
        doc["query_embedding"] = embed_fn(SEED_CONFIG["query_text"])
        doc["updated_at"] = _now_iso()
        await db.config.insert_one(doc)
        return True
    return False
