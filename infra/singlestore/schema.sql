-- ============================================================================
-- Schema para papers-classifier-singlestore
-- Ejecutar UNA SOLA VEZ en tu workspace SingleStore Cloud despues de crear el
-- database. Conexion mas simple: portal web → Data Studio → ejecutar este SQL.
--
-- Asumimos que el database ya existe. Si no:
--   CREATE DATABASE examen_papers;
--   USE examen_papers;
-- ============================================================================

CREATE TABLE IF NOT EXISTS papers (
  paper_id        VARCHAR(20) PRIMARY KEY,
  title           TEXT,
  abstract        TEXT,
  text_full       TEXT,
  year            INT,

  -- Embedding SBERT all-MiniLM-L6-v2 (384 dimensiones, float32).
  -- VECTOR es un tipo nativo de SingleStore con DOT_PRODUCT optimizado en
  -- columnar engine.
  embedding       VECTOR(384) NOT NULL,

  -- Scores individuales: computados al ingest y opcionalmente recomputados
  -- por el reclassify SQL.
  score_keyword   DOUBLE DEFAULT 0,
  score_tfidf     DOUBLE DEFAULT 0,
  score_sbert     DOUBLE DEFAULT 0,
  score_weighted  DOUBLE DEFAULT 0,

  -- Decision final tras combinacion ponderada + umbrales.
  score_final     INT DEFAULT 0,
  decision        VARCHAR(50),

  -- Justificacion narrativa generada por Ollama (lazy/on-demand).
  justification   TEXT,

  -- MinIO keys. El upload es inmutable; tier_key es la copia en gold/silver/etc.
  minio_key       VARCHAR(300),
  tier_minio_key  VARCHAR(300),

  created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,

  -- Indice fulltext para MATCH AGAINST (TF-IDF nativo del fulltext engine).
  FULLTEXT INDEX ft_text (text_full),

  -- Indices secundarios para filtros del dashboard.
  KEY idx_score (score_final),
  KEY idx_year (year)
) ENGINE = COLUMNSTORE;
