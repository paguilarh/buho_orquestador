#!/usr/bin/env bash
# Crea las 3 service accounts que Airflow necesita, una por proyecto de GCP.
#
# Correr en Cloud Shell (ya trae gcloud autenticado):
#     bash crear_service_accounts.sh
#
# Es idempotente: si una SA ya existe, la reutiliza en vez de fallar.
# Genera las llaves en ./llaves/ — NO las subas a git.
set -euo pipefail

DESTINO_LLAVES="./llaves"

# proyecto | nombre de la SA | connection de Airflow
CUENTAS=(
  "arquim|airflow-media|gcp_media"
  "arquimides-441521|airflow-logistics|gcp_logistics"
  "b-materials|airflow-entradas-salidas|gcp_entradas_salidas"
)

# Mínimo necesario: correr queries, escribir en BQ, disparar Cloud Run Jobs.
# Deliberadamente NO se otorga editor ni owner — el diseño depende de que
# cada llave esté acotada a su proyecto.
ROLES=(
  "roles/bigquery.jobUser"
  "roles/bigquery.dataEditor"
  "roles/run.developer"
)

mkdir -p "$DESTINO_LLAVES"
chmod 700 "$DESTINO_LLAVES"

for entrada in "${CUENTAS[@]}"; do
  IFS='|' read -r PROYECTO NOMBRE CONN <<< "$entrada"
  SA="${NOMBRE}@${PROYECTO}.iam.gserviceaccount.com"

  echo ""
  echo "=============================================================="
  echo "  $PROYECTO  →  $NOMBRE  (connection: $CONN)"
  echo "=============================================================="

  if gcloud iam service-accounts describe "$SA" --project="$PROYECTO" >/dev/null 2>&1; then
    echo "  [=] la service account ya existe, se reutiliza"
  else
    echo "  [+] creando service account"
    gcloud iam service-accounts create "$NOMBRE" \
      --project="$PROYECTO" \
      --display-name="Airflow self-hosted — $PROYECTO"
  fi

  for ROL in "${ROLES[@]}"; do
    echo "  [+] otorgando $ROL"
    gcloud projects add-iam-policy-binding "$PROYECTO" \
      --member="serviceAccount:${SA}" \
      --role="$ROL" \
      --condition=None \
      --quiet >/dev/null
  done

  LLAVE="${DESTINO_LLAVES}/${NOMBRE}.json"
  if [[ -f "$LLAVE" ]]; then
    echo "  [=] la llave ya existe en $LLAVE, no se genera otra"
  else
    echo "  [+] generando llave en $LLAVE"
    gcloud iam service-accounts keys create "$LLAVE" \
      --iam-account="$SA" \
      --project="$PROYECTO"
    chmod 600 "$LLAVE"
  fi
done

echo ""
echo "=============================================================="
echo "  Listo. Siguientes pasos — se hacen a mano:"
echo "=============================================================="
cat <<'PASOS'

  1. Subir las llaves al droplet:

       scp llaves/*.json root@<IP-DEL-DROPLET>:/opt/airflow/secrets/
       ssh root@<IP-DEL-DROPLET> 'chmod 600 /opt/airflow/secrets/*.json'

  2. Borrar las copias locales en cuanto estén arriba:

       rm -rf llaves/

  3. Crear las Connections en el droplet (cd /opt/buho_orchestration):

       docker compose -f infra/docker-compose.yml exec airflow-scheduler \
         airflow connections add gcp_media \
           --conn-type google_cloud_platform \
           --conn-extra '{"key_path":"/opt/airflow/secrets/airflow-media.json","project":"arquim"}'

       docker compose -f infra/docker-compose.yml exec airflow-scheduler \
         airflow connections add gcp_logistics \
           --conn-type google_cloud_platform \
           --conn-extra '{"key_path":"/opt/airflow/secrets/airflow-logistics.json","project":"arquimides-441521"}'

       docker compose -f infra/docker-compose.yml exec airflow-scheduler \
         airflow connections add gcp_entradas_salidas \
           --conn-type google_cloud_platform \
           --conn-extra '{"key_path":"/opt/airflow/secrets/airflow-entradas-salidas.json","project":"b-materials"}'

  4. Verificar:

       docker compose -f infra/docker-compose.yml exec airflow-scheduler \
         airflow connections list --output table

PASOS
