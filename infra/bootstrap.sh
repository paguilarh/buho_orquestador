#!/usr/bin/env bash
# Provisión de un droplet vacío hasta dejar Airflow corriendo.
# Uso:  ./bootstrap.sh <url-del-repo>
set -euo pipefail

REPO_URL="${1:?Falta la URL del repo}"
DESTINO=/opt/buho_orchestration

echo "==> Instalando Docker"
apt-get update
apt-get install -y ca-certificates curl git
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  > /etc/apt/sources.list.d/docker.list
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

echo "==> Preparando el directorio de secretos"
mkdir -p /opt/airflow/secrets
chmod 700 /opt/airflow/secrets

echo "==> Clonando el repo"
git clone "$REPO_URL" "$DESTINO"

echo ""
echo "==> Listo. Faltan pasos manuales que requieren secretos:"
echo "    1. cp $DESTINO/.env.example $DESTINO/.env"
echo "    2. llenar los valores de $DESTINO/.env"
echo "    3. copiar las 3 llaves SA a /opt/airflow/secrets/ con chmod 600"
echo "    4. cd $DESTINO && docker compose -f infra/docker-compose.yml up -d"
