#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-.env}"

SRC_DIR="${SRC_DIR:-/home/deployzdg/zpro.io/backend/public}"   # contém 1..7
DST_DIR="${DST_DIR:-/New-Omni/backend/public/media}"           # destino final

if [[ ! -f "$ENV_FILE" ]]; then
  echo "❌ Não achei o .env em: $ENV_FILE"
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

need() {
  local v="$1"
  if [[ -z "${!v:-}" ]]; then
    echo "❌ Variável obrigatória não definida no .env: $v"
    exit 1
  fi
}

need SRC_SSH_HOST
need SRC_SSH_USER
need SRC_SSH_PORT
need SRC_SSH_PASSWORD

need DST_SSH_HOST
need DST_SSH_USER
need DST_SSH_PORT
need DST_SSH_PASSWORD

echo "=============================="
echo "Origem : $SRC_SSH_USER@$SRC_SSH_HOST:$SRC_DIR"
echo "Destino: $DST_SSH_USER@$DST_SSH_HOST:$DST_DIR"
echo "Modo   : DIRECT (origem ➜ destino) — EDU não armazena nada"
echo "=============================="

# 1) garante que o destino exista (executa no DESTINO)
echo "📁 Garantindo pasta de destino..."
sshpass -p "$DST_SSH_PASSWORD" ssh -o StrictHostKeyChecking=no -p "$DST_SSH_PORT" \
  "$DST_SSH_USER@$DST_SSH_HOST" "mkdir -p '$DST_DIR'"

# 2) executa rsync NO SERVIDOR DE ORIGEM, empurrando pro destino
echo "🚚 Transferindo direto (rsync rodando no servidor de origem)..."
sshpass -p "$SRC_SSH_PASSWORD" ssh -o StrictHostKeyChecking=no -p "$SRC_SSH_PORT" \
  "$SRC_SSH_USER@$SRC_SSH_HOST" "bash -lc '
    set -euo pipefail

    if ! command -v rsync >/dev/null 2>&1; then
      echo \"❌ rsync não está instalado no servidor origem. Instale com: apt install -y rsync\"
      exit 1
    fi

    rsync -a --info=progress2 --human-readable --partial --append-verify \
      -e \"ssh -o StrictHostKeyChecking=no -p $DST_SSH_PORT\" \
      \"$SRC_DIR/1\" \"$SRC_DIR/2\" \"$SRC_DIR/3\" \"$SRC_DIR/4\" \"$SRC_DIR/5\" \"$SRC_DIR/6\" \"$SRC_DIR/7\" \
      \"$DST_SSH_USER@$DST_SSH_HOST:$DST_DIR/\"
  '"

echo "✅ Concluído (direct)."
