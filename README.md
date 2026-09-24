# buho_orchestration

Orquestación de los pipelines de datos de Buho — Airflow 2.10.5 self-hosted.

## Documentación

| Documento | Qué contiene |
|---|---|
| `docs/2026-09-23-orquestador-airflow-design.md` | diseño y orden de migración |
| `docs/2026-09-23-orquestador-airflow-fase0-plan.md` | plan de implementación de la fase 0 |

Los pipelines que este repo orquesta viven en el monorepo `buho_data_pipelines`.

## Desarrollo local

Airflow no corre nativamente en Windows. Todo pasa por Docker:

    docker compose -f infra/docker-compose.yml --profile test run --rm airflow-test pytest tests/ -v

## Despliegue

Merge a `main` dispara `.github/workflows/deploy.yml`.
