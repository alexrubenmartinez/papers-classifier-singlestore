"""Cliente SingleStore para el storage de papers (con embeddings).

singlestoredb es sincronico; envolvemos cada query en asyncio.to_thread para no
bloquear el event loop de FastAPI. Los queries SQL son cortos (decenas de ms),
el overhead del thread switch es despreciable.

Por que SingleStore en lugar de seguir con Mongo:
- DOT_PRODUCT(embedding, query_emb) ejecutado en engine columnar: 2000 rows / ~50 ms.
- MATCH(text_full) AGAINST(keywords): fulltext nativo, ~20 ms.
- Reclassify completo: 1 sentencia UPDATE = ~1.5 s vs 30 s en Python loop.
"""
import asyncio
import os
from typing import Any

import singlestoredb as s2

SINGLESTORE_URL = os.getenv(
    "SINGLESTORE_URL",
    "",  # Sin default: si no esta, falla rapido y claro.
)


class SinglestorePool:
    """Wrapper minimo. singlestoredb maneja su propio pool internamente."""

    def __init__(self, url: str):
        if not url:
            raise RuntimeError(
                "SINGLESTORE_URL no configurado. Setealo en .env apuntando a tu "
                "workspace Cloud (formato mysql://user:pass@host:port/db)."
            )
        self.url = url

    def _conn(self):
        return s2.connect(self.url)

    async def execute(self, sql: str, params: tuple | None = None) -> int:
        """INSERT/UPDATE/DELETE. Devuelve rowcount."""
        def _run():
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params or ())
                    conn.commit()
                    return cur.rowcount

        return await asyncio.to_thread(_run)

    async def execute_many(self, sql: str, params_list: list[tuple]) -> int:
        """Bulk INSERT/UPDATE. Usar para insert_bulk en lotes de 100."""
        def _run():
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(sql, params_list)
                    conn.commit()
                    return cur.rowcount

        return await asyncio.to_thread(_run)

    async def fetchone(self, sql: str, params: tuple | None = None) -> dict[str, Any] | None:
        def _run():
            with self._conn() as conn:
                with conn.cursor(dictionary=True) as cur:
                    cur.execute(sql, params or ())
                    return cur.fetchone()

        return await asyncio.to_thread(_run)

    async def fetchall(self, sql: str, params: tuple | None = None) -> list[dict[str, Any]]:
        def _run():
            with self._conn() as conn:
                with conn.cursor(dictionary=True) as cur:
                    cur.execute(sql, params or ())
                    return list(cur.fetchall())

        return await asyncio.to_thread(_run)


_pool: SinglestorePool | None = None


def get_singlestore() -> SinglestorePool:
    """Singleton del pool. Lazy init: solo se crea al primer uso."""
    global _pool
    if _pool is None:
        _pool = SinglestorePool(SINGLESTORE_URL)
    return _pool


async def ping() -> bool:
    """Liveness check para el endpoint /health."""
    try:
        await get_singlestore().fetchone("SELECT 1 AS ok")
        return True
    except Exception:
        return False
