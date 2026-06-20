# papers-classifier-singlestore

Pipeline de clasificación de papers académicos con **SingleStore como vector store** para acelerar dramáticamente el reclassify (1 sentencia SQL en vez de loop Python) y **multiprocessing en el VPS** para acelerar el ingest inicial.

Es la versión "vectorizada/optimizada" del proyecto. La versión anterior (Mongo-only) sigue funcional en [`papers-classifier-llm`](https://github.com/alexrubenmartinez/papers-classifier-llm).

## Tiempos esperados (vs versión Mongo-only)

| Operación | Mongo-only | Este repo |
|---|---|---|
| Ingest 2000 papers | ~15 min | **~3 min** (5× multiproc) |
| Reclassify (cambio de tema) | ~30 s | **~1.5 s** (1 UPDATE SQL) |
| Ranking top-N | ~50 ms | ~5 ms (columnar) |
| Filtros tier/year/score | ~50 ms | ~10 ms |

## Arquitectura

```
                    ┌──────────────────────────┐
                    │  Browser / Postman       │
                    └─────────────┬────────────┘
                                  │ HTTPS
                                  ▼
       ┌────────────────────────────────────────────────────┐
       │  nginx central (cert wildcard)                      │
       │   ui.*       → papers-ui                            │
       │   api.*      → papers-api  (con API key check)      │
       │   ollama-api.*→ ollama     (con API key check)      │
       └─────────────────┬───────────────┬──────────────────┘
                         │               │
              ┌──────────▼──┐    ┌───────▼─────────┐
              │  papers-ui  │    │   papers-api    │
              │             │    │   (FastAPI)     │
              │ Proxy /api/ │───▶│  ProcessPool×N  │
              │ con X-API-  │    │  + asyncio loop │
              │ Key inyect. │    │                 │
              └─────────────┘    └─┬─────┬─────┬───┘
                                   │     │     │
                ┌──────────────────▼┐ ┌──▼──┐ ┌▼──────┐
                │   SingleStore     │ │Mongo│ │ MinIO │
                │   Cloud (free)    │ │ jobs│ │ PDFs +│
                │                   │ │  +  │ │ tier  │
                │   papers          │ │cfg  │ │folders│
                │   - embedding     │ │     │ │       │
                │     VECTOR(384)   │ │     │ │       │
                │   - text_full     │ │     │ │       │
                │     FULLTEXT IDX  │ │     │ │       │
                │   - scores        │ │     │ │       │
                └───────────────────┘ └─────┘ └───────┘

Reclassify = UPDATE papers SET sbert_score = DOT_PRODUCT(...),
                                text_score  = MATCH AGAINST(...), ...
            → 1.5 s para 2000 papers (vs 30 s en Python loop).
```

## Pre-requisitos

### En tu VPS
- Linux + Docker + Docker Compose v2.
- Stack `stack_web` con `mongodb`, `minio`, `nginx` activos.
- Cert wildcard `*.tu-vps.example.com`.
- 23+ GB RAM (Ollama + N × SBERT model).

### En SingleStore Cloud (free)
- Crear cuenta en [portal.singlestore.com](https://portal.singlestore.com/).
- Crear un workspace (free tier S-00).
- Crear database `papers_corpus` (o el nombre que prefieras).
- Ejecutar `infra/singlestore/schema.sql` desde el SQL Editor.
- Anotar el endpoint, usuario y password del workspace.

## Pasos para replicar

### 1. Clonar y configurar

```bash
git clone https://github.com/<owner>/papers-classifier-singlestore.git
cd papers-classifier-singlestore
cp .env.example .env
$EDITOR .env
```

Variables clave a completar:

```bash
VPS_DOMAIN=tu-vps.example.com
API_KEY=$(openssl rand -hex 32)
DB_PASSWORD=tu-password-de-mongo-minio
SINGLESTORE_URL=mysql://admin:tu-pass-singlestore@svc-xxx.svc.singlestore.com:3306/papers_corpus
```

### 2. Aplicar la configuración

```bash
set -a && source .env && set +a
bash infra/configure.sh
```

El script valida que `SINGLESTORE_URL` empiece con `mysql://`, reemplaza placeholders en los compose files y nginx vhosts.

### 3. Crear el schema en SingleStore (UNA sola vez)

Desde el portal SingleStore → SQL Editor, pegá y ejecutá el contenido de `infra/singlestore/schema.sql`. La tabla `papers` queda creada con `embedding VECTOR(384)` y `FULLTEXT INDEX`.

### 4. Deploy en el VPS

```bash
rsync -avz --exclude='node_modules' --exclude='dist' ./ root@$VPS_HOST:/root/papers-ss/

ssh root@$VPS_HOST
cd /root/papers-ss

# Ollama
cd infra/ollama && docker compose up -d && docker exec ollama ollama pull qwen2.5:1.5b

# API (tarda ~8 min: torch CPU + sentence-transformers + pre-download SBERT)
cd /root/papers-ss/api && docker compose up -d --build

# UI
cd /root/papers-ss/ui && docker compose up -d --build

# nginx vhosts
cp /root/papers-ss/infra/nginx/*.conf /root/stack/data/nginx/conf.d/
docker exec nginx nginx -t && docker exec nginx nginx -s reload
```

### 5. Verificar

```bash
curl https://papers-api.$VPS_DOMAIN/health
# → { ... "singlestore_reachable": true, "workers": N, ... }

# Importar el corpus (los 2000 PDFs del classifier-g3 original)
curl -H "X-API-Key: $API_KEY" -X POST https://papers-api.$VPS_DOMAIN/papers/import \
  -H "Content-Type: application/json" \
  -d '{"source_prefix": "grupo3_ciberseguridad/bronze/papers/"}'

# Monitorear job (devuelve elapsed_seconds + rate)
curl -H "X-API-Key: $API_KEY" https://papers-api.$VPS_DOMAIN/jobs/<JOB_ID>
```

## Endpoints (idénticos a la versión anterior)

| Método | Path | Notas vs Mongo-only |
|---|---|---|
| GET | `/health` | Agrega `papers_by_tier`, `singlestore_reachable`, `workers` |
| POST | `/papers` | Upload individual (single-process, sin cambios visibles) |
| POST | `/papers/import` | **Paralelizado** con `ProcessPoolExecutor(N=cpu-1)` |
| POST | `/reclassify` | **1 UPDATE SQL** + tier folder sync. Type del job: `reclassify_sql` |
| PUT | `/config?reclassify=true` | Igual al anterior; dispara el reclassify SQL |
| GET | `/papers?limit&min_score` | Reads desde SingleStore (columnar = fast) |
| GET | `/ranking?top` | `ORDER BY score_weighted DESC LIMIT N` |
| POST | `/justify/:id` | Igual al anterior (Ollama, single-call) |
| GET | `/jobs/:id`, `/stream` | Igual al anterior |
| POST | `/chat` | Igual al anterior (proxy Ollama) |

OpenAPI auto-generado en `https://papers-api.$VPS_DOMAIN/docs`.

## Tier folders en MinIO

```
examen-parcial/papers-api/
  ├── uploads/         (bronze inmutable — el PDF original)
  ├── gold/            (score 4-5)
  ├── silver/          (score 3)
  ├── bronze/          (score 1-2)
  └── out_of_range/    (score 0)
```

Tras cada reclassify, el tier sync mueve los PDFs entre folders donde `expected_tier_key != tier_minio_key`. Idempotente.

## Verificación de SingleStore desde la CLI

Útil para debuggear scoring:

```bash
mysql -h svc-xxx.svc.singlestore.com -u admin -p papers_corpus \
  -e "SELECT paper_id, title, score_final, decision,
             (DOT_PRODUCT(embedding, JSON_ARRAY_PACK('[0.1, ...]')) + 1)/2 AS sim
      FROM papers ORDER BY sim DESC LIMIT 10;"
```

(Reemplazá el array con el embedding real de tu query — lo podés obtener llamando a `/config` que ya lo trae cacheado en Mongo.)

## Troubleshooting

### `singlestore_reachable: false` en /health

Verificá que `SINGLESTORE_URL` esté bien formado (`mysql://user:pass@host:port/db`) y que el workspace esté activo en el portal.

### El `_run_import_batch` cuelga

Probablemente uno de los workers crasheó con un PDF mal formado. Mirá los logs:

```bash
docker logs -f papers-api | grep -E "import|process_one|tier"
```

### `FULLTEXT INDEX` no devuelve resultados

SingleStore requiere que los textos pasen un ratio de "stop-word density" mínimo. Si las keywords son muy genéricas o el texto es muy corto, `MATCH AGAINST` puede devolver 0. Verificá con:

```sql
SELECT MATCH(text_full) AGAINST ('zero trust') AS rel FROM papers LIMIT 5;
```

## Out of scope

- Migración desde el repo anterior (`papers-classifier-llm`). Este repo empieza limpio.
- Migrar `jobs` y `config` a SingleStore (siguen en Mongo).
- Justify con Ollama acelerado (sigue siendo ~15 s/paper en CPU).

## Licencia

MIT
