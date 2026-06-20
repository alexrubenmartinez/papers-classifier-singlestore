#!/usr/bin/env bash
# infra/configure.sh
#
# Aplica las variables del .env a los archivos de despliegue
# (nginx vhosts + ui/nginx.conf + compose files).
#
# Uso:
#   set -a && source .env && set +a
#   bash infra/configure.sh
#
# Idempotente para placeholders; no revierte cambios. Recomendado correrlo
# sobre un repo recién clonado.

set -euo pipefail

cd "$(dirname "$0")/.."

# ---------------------------------------------------------------------------
# Validación de variables requeridas
# ---------------------------------------------------------------------------
required=(API_KEY VPS_DOMAIN DB_PASSWORD SINGLESTORE_URL)
missing=()
for v in "${required[@]}"; do
  if [ -z "${!v:-}" ]; then
    missing+=("$v")
  fi
done

if [ ${#missing[@]} -ne 0 ]; then
  echo "ERROR: faltan variables en el .env: ${missing[*]}" >&2
  echo "       Asegúrate de hacer 'set -a && source .env && set +a' antes." >&2
  exit 1
fi

# Sanity check del formato SingleStore URL.
if [[ ! "$SINGLESTORE_URL" =~ ^mysql:// ]]; then
  echo "ERROR: SINGLESTORE_URL debe empezar con 'mysql://'." >&2
  echo "       Ej: mysql://admin:pass@svc-xxx.svc.singlestore.com:3306/examen_papers" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Archivos a parchar y reemplazos
# ---------------------------------------------------------------------------
files=(
  "infra/nginx/06-ollama-api.conf"
  "infra/nginx/11-papers-api.conf"
  "infra/nginx/12-papers-ui.conf"
  "ui/nginx.conf"
  "ui/docker-compose.yml"
  "infra/ollama/docker-compose.yml"
  "api/docker-compose.yml"
)

# Escapar caracteres especiales del URL (perl interpola). Usamos | como
# separador de regex para evitar conflicto con / del path.
escape_for_perl() {
  printf '%s' "$1" | sed 's/[\/&|]/\\&/g'
}

API_KEY_ESC=$(escape_for_perl "$API_KEY")
DB_PASSWORD_ESC=$(escape_for_perl "$DB_PASSWORD")
SINGLESTORE_URL_ESC=$(escape_for_perl "$SINGLESTORE_URL")
VPS_DOMAIN_ESC=$(escape_for_perl "$VPS_DOMAIN")

for f in "${files[@]}"; do
  if [ ! -f "$f" ]; then
    echo "warn: $f no existe, salteado" >&2
    continue
  fi
  perl -i -pe "
    s|REPLACE_API_KEY|${API_KEY_ESC}|g;
    s|REPLACE_PASSWORD|${DB_PASSWORD_ESC}|g;
    s|REPLACE_SINGLESTORE_URL|${SINGLESTORE_URL_ESC}|g;
    s|your-vps\\.example\\.com|${VPS_DOMAIN_ESC}|g;
  " "$f"
  echo "  ✓ $f"
done

echo
echo "Configuración aplicada. Próximos pasos:"
echo "  1. Ejecutar infra/singlestore/schema.sql en tu workspace SingleStore Cloud (UNA VEZ)."
echo "  2. En el VPS: docker compose up -d --build  (en cada uno de: infra/ollama, api, ui)"
echo "  3. Copiar infra/nginx/*.conf al nginx central del stack y reload."
