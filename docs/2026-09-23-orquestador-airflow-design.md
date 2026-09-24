# Diseño — Orquestador Airflow self-hosted

<!-- CUANDO LEER: antes de implementar el orquestador · al cuestionar por qué se eligió DO sobre GCE -->
<!-- NO NECESARIO PARA: operar DAGs del día a día — para eso está el README de buho_orchestration -->

> Diseño validado en sesión 2026-09-23. Este documento explica el porqué y el orden de
> migración; la implementación vive en este mismo repo.
>
> Los pipelines que se migran viven en el monorepo `buho_data_pipelines`, y las rutas
> `media/...` y `logistics/...` que se citan aquí son relativas a ese repo.
>
> **v2 — auditada.** Esta versión incorpora 15 hallazgos de dos auditorías independientes
> (Claude + minero). Ver §9 para la traza. La v1 contenía dos errores que habrían roto
> producción; no usar ninguna copia anterior a esta.

---

## 1. Problema

Hoy hay ~15 procesos en producción, cada uno disparado por su propio Cloud Scheduler,
sin dependencias entre ellos y sin un lugar único donde ver qué corrió y qué falló.
Tres dolores concretos, declarados por el usuario:

| Dolor | Cómo se manifiesta |
|---|---|
| Dependencias entre jobs | los Schedulers disparan por hora, a ciegas — no hay "corre TRF solo si el loader terminó bien" |
| Observabilidad | N Cloud Run Services y Jobs sin monitoreo claro; enterarse de un fallo exige mirar Cloud Logging a mano |
| Procesos manuales | los reportes de `kpis_wms_batch` se corren en la laptop, sin programación |

**Hallazgo que reencuadra el proyecto:** ya existe un Cloud Composer vivo y facturando
(`buho-claw-environment`, Composer 3 / Airflow 2.10.5, `ENVIRONMENT_SIZE_SMALL`, proyecto
`b-materials`), provisionado en `logistics/app_entradas_salidas/terraform/main.tf:145`.
El proyecto no es "montar un orquestador nuevo" sino **migrar de Composer a self-hosted y
consolidar los procesos dispersos**, con el costo de Composer como caso de negocio.

## 2. Inventario de procesos

### Media — `media/data_lake_produccion`

Un solo Scheduler dispara una cadena de 5 pasos y **dos loaders fire-and-forget**. Ese
disparo a ciegas, sin saber si el downstream terminó, es exactamente lo que Airflow corrige.

| Proceso | Trigger actual | Notas | Destino |
|---|---|---|---|
| `loader_jobs` (cadena de 5 pasos) | Scheduler `0 9,13,19 * * 1-5` | llama por HTTP a `trf/` y dispara los dos loaders de abajo | Fase 5 |
| `loader_job_xml` | disparado por `loader_jobs` | fire-and-forget · sin control de fallo | Fase 5 |
| `loader_documents` | disparado por `loader_jobs` | fire-and-forget · sin control de fallo | Fase 5 |
| `loader_device_status` | Scheduler `0 * * * *` | horario, todos los días | Fase 5 |

### Logistics — `logistics/kpis_wms`

| Proceso | Trigger actual | Notas | Destino |
|---|---|---|---|
| webhook → Cloud Tasks → `/trf/orders\|shipments\|packages` | evento (ShipStream) | **fuera de alcance** | — |
| `/trf/manifiestos` | Scheduler 3x/día L-V (11:30, 15:00, 21:00 MTY) | mismo Service que el webhook — ver §3.5 | Fase 4 |
| `etl_sheets` → `/load/tiempos/diario` | Scheduler diario | MERGE inline | Fase 4 |
| `etl_sheets` → `/load/tiempos/semanal` | Scheduler `0 7 * * 1` (lunes 7 AM MTY) | MERGE inline | Fase 4 |
| `etl_sheets` → `/load/quejas` | Scheduler programado | MERGE inline | Fase 4 |
| `locations` (Job) | Scheduler `0 22 * * 1-6` MTY | **MERGE inline** | Fase 4 |
| `movement_log` (Job) | Scheduler diario (`0 4 * * *` UTC = 22:00 MTY) | **MERGE inline** | Fase 4 |
| `tracking_events` (Job) | Scheduler `0 12,22 * * 1-5` MTY | **MERGE inline** | Fase 4 |
| `asn` (Job, KPI #9) | Scheduler diario 22:00 MTY | **MERGE inline** | Fase 4 |
| `reports/last_mile_prediction_wms` | Scheduler 1x/trimestre | ver §8 | fuera de alcance |

### Logistics — otros

| Proceso | Trigger actual | Notas | Destino |
|---|---|---|---|
| `in_out_analytics_pipeline` | Composer (diario `30 15 * * *` = 9:30 MTY) | **dos** Cloud Run Jobs: `wms-in-out-analytics` + `in-out-analytics-dbt-job` con overrides de `DBT_MODEL`/`DBT_TARGET` | Fase 1 |
| `kpis_wms_batch/reports/analisis_inventario_estancado` | manual | automatizable | Fase 3 |
| `kpis_wms_batch/reports/wing_heatmap` | manual | **requiere Excel descargado a mano del portal Wing** — ver §5 fase 3 | Fase 3 (parcial) |
| `kpis_wms_batch/scripts/batch_*.py` | manual | fallbacks para re-correr cuando un Scheduler falla | se retiran — ver §8 |

## 3. Decisiones de arquitectura

### 3.1 Hosting — DigitalOcean primero, GCE después

| Opción | Costo/mes | Autenticación contra BQ |
|---|---|---|
| Cloud Composer (actual) | ver §6 | IAM nativo |
| GCE `e2-medium` | ver §6 | ADC nativo — **cero llaves** |
| DigitalOcean 4 GB | ver §6 | llave SA JSON en disco |

**Decisión: empezar en DigitalOcean, containerizado para que migrar a GCE sea cambiar el host.**
Las llaves SA se aceptan como **deuda técnica documentada**, no como descuido. Mitigación en §7.

El argumento a favor de GCE (paridad de precio, sin llaves que rotar, misma red que BigQuery)
queda registrado porque sigue siendo válido: el beneficio de DO sería independencia de GCP,
pero los datos viven en BigQuery — si GCP cae, un Airflow en DO no tiene con quién hablar.

### 3.2 Dimensionamiento — 4 GB, no 2 GB

La propuesta inicial era un droplet de 2 GB. No alcanza: Airflow con LocalExecutor levanta
scheduler + webserver + Postgres, que consumen ~2-2.5 GB en reposo. Con loaders corriendo
dentro del worker (modelo híbrido, §3.3) el piso realista es **4 GB**.

Los 4 GB sí alcanzan porque el cómputo pesado no ocurre en la VM: el SQL corre en BigQuery
vía operator y el worker solo espera.

> Las cifras de consumo en reposo son estimación, no medición. Validar en fase 0.

### 3.3 Modelo híbrido — regla de corte

Airflow no reemplaza todo el cómputo de Cloud Run. La regla que evita que el híbrido se
vuelva un desorden, en orden de precedencia — **la primera que aplica, manda**:

| # | Condición | Dónde corre |
|---|---|---|
| 1 | La tarea es event-driven (webhook, Cloud Tasks) | se queda en Cloud Run Service · Airflow no la toca |
| 2 | El loader ya hace **MERGE inline** | `CloudRunExecuteJobOperator` — Airflow **no** re-ejecuta su SQL |
| 3 | Es SQL de TRF/CONSUMPTION que hoy corre en un Service | `BigQueryInsertJobOperator` |
| 4 | Tarea < 5 min, < 512 MB, sin deps de sistema | `PythonOperator` en el worker |
| 5 | Cualquier otra cosa | `CloudRunExecuteJobOperator` |

**La regla 2 es la que evita duplicar trabajo.** `locations`, `movement_log`, `tracking_events`
y `asn` ya ejecutan su propio MERGE al terminar de cargar RAW. Si Airflow además corriera ese
SQL con un operator, el MERGE se ejecutaría dos veces por corrida. Es idempotente, así que no
corrompe datos — pero es costo de BigQuery tirado a la basura y una fuente de confusión al
depurar. Para esos cuatro, Airflow **solo dispara el Job**.

**La regla 4 no tiene excepciones por categoría.** Un reporte hoy manual no califica para
`PythonOperator` por ser reporte: califica si cumple el umbral. `last_mile_prediction_wms`
llama a la API de Google Maps y hace optimización de rutas — no lo cumple, y por eso está
fuera de alcance (§8).

### 3.4 Topología

```
┌─────────────────── DigitalOcean droplet 4GB ───────────────────┐
│  Caddy (TLS) → Airflow webserver                               │
│  Scheduler · Worker (LocalExecutor)                            │
│  Postgres 16 (metadata)  ·  Docker Compose                     │
└────────────────────────┬───────────────────────────────────────┘
                         │  3 Airflow Connections · 1 SA por proyecto (§3.7)
          ┌──────────────┼──────────────┬─────────────────┐
          ▼              ▼              ▼                 ▼
   Cloud Run Jobs   BigQuery      APIs externas    Google Sheets
   (loaders         (SQL TRF /    (WMS, Wing,
    pesados)         CONSUMPTION)  PrintFactory)

        FUERA DEL ALCANCE — sigue event-driven sin Airflow:
        webhook WMS → Cloud Tasks → kpis-wms-trf (/orders /shipments /packages)
```

### 3.5 SQL cross-repo — qué migra y qué se queda

Los DAGs viven en `buho_orchestration`, no en este monorepo, y el SQL de TRF vive hoy en
`media/data_lake_produccion/sql/` y `logistics/kpis_wms/sql/`.

**Regla: el SQL de procesos batch migra; el de eventos permanece en su servicio.**

- En `media/data_lake_produccion`, el Cloud Run Service `trf/` se apaga al completarse la
  fase 5: todos sus triggers son temporales y su SQL migra a `include/sql/`.
- En `logistics/kpis_wms`, el Cloud Run Service `kpis-wms-trf` **permanece encendido**.
  `trf/main.py:160-178` sirve `/trf/orders`, `/trf/shipments` y `/trf/packages` — los
  endpoints del webhook, declarado fuera de alcance — **en el mismo servicio** que
  `/trf/manifiestos`. Solo la ejecución programada de `/trf/manifiestos` migra a Airflow,
  junto con `trf_manifiestos.sql`. Apagar ese Service rompería el pipeline en tiempo real
  de ShipStream.

Se muda, no se duplica: nunca hay dos copias vivas del mismo SQL.

### 3.6 Versión de Airflow — 2.10.x

**Decisión: arrancar en 2.10.x, la misma que corre Composer hoy.**

`in_out_analytics_pipeline_dag.py:16` usa `schedule_interval`, parámetro removido en Airflow
3.0 (renombrado `schedule`). En 2.10.x el DAG migra sin editar una línea y la fase 1 es una
prueba limpia de paridad: cambia el host, nada más.

Subir a 3.x queda como fase posterior, con su propia validación. Un cambio a la vez.

### 3.7 Service accounts — una por proyecto

Los pipelines tocan **tres proyectos de GCP**: `arquim`, `arquimides-441521` y `b-materials`.

**Decisión: una SA por proyecto, tres Airflow Connections** (`gcp_media`, `gcp_logistics`,
`gcp_entradas_salidas`). Son tres llaves que administrar en lugar de una, pero cada una
limitada a su proyecto: si una se filtra, el daño está acotado a ese ámbito. Una SA central
con roles cruzados concentraría los tres proyectos en una sola llave.

Cada SA recibe solo los roles de los datasets y Jobs que su área toca. El inventario exacto
de roles se define en fase 0.

### 3.8 Timezone — `America/Monterrey`

Hay conflicto hoy: Composer está configurado en `America/Mexico_City`
(`terraform/main.tf:162`) mientras el estándar de `kpis_wms` es `America/Monterrey`
(`docs/arquitectura.md:517`). Ambos son UTC-6 sin horario de verano, así que hoy no hay
diferencia de comportamiento — pero son identificadores distintos y la ambigüedad se
resuelve ahora, no cuando alguien depure un corte de día a las 2 AM.

**`America/Monterrey` como default global**, por coherencia con el estándar de `kpis_wms`,
que es el grueso de los procesos. El DAG de `in_out` se alinea al migrar en fase 1.

## 4. Estructura del repo

```
buho_orchestration/
├── dags/
│   ├── media/            ← un archivo por loader, no uno por proyecto
│   └── logistics/
├── plugins/buho/
│   ├── defaults.py      ← default_args estándar (retries, timezone MTY, callbacks)
│   ├── callbacks.py     ← on_failure → Slack + correo, en un solo lugar
│   └── operators/bq.py  ← wrapper de BigQueryInsertJobOperator
├── include/sql/         ← SQL migrado desde los repos de origen
├── tests/test_dag_integrity.py
├── infra/               ← docker-compose · Caddyfile · bootstrap.sh
└── .github/workflows/   ← ci.yml (lint+tests) · deploy.yml (merge → VM)
```

`media/` no cabe en un solo archivo DAG: son cuatro loaders con dependencias distintas y un
Scheduler horario independiente. Un archivo por unidad de programación, no por proyecto.

### Componentes reutilizables

| Componente | Responsabilidad |
|---|---|
| `defaults.py` | `buho_default_args(owner, area)` — reintentos, timezone `America/Monterrey`, callback de fallo ya enganchado. Cambiar la política de todos los pipelines es editar una línea. |
| `callbacks.py` | `notify_failure(context)` — arma el mensaje (dag_id, task_id, link al log, fecha) y lo manda a Slack y correo. Ningún DAG reimplementa notificación. |
| `operators/bq.py` | `BuhoBigQueryOperator` — carga el SQL desde `include/sql/` por nombre, inyecta `batch_id`, `ds` y el **project ID por Connection** (§3.7), y pone labels de BQ para rastrear costo por pipeline. Los DAGs nombran SQL, no lo embeben. |
| `test_dag_integrity.py` | Hace cumplir el estándar sin depender de disciplina: falla el CI si hay error de import, ciclo, falta un tag obligatorio, falta `on_failure_callback`, o `catchup` no está explícito. |

El SQL del monorepo usa placeholders de project ID de forma inconsistente. Al migrar cada
archivo a `include/sql/`, el project ID se parametriza vía la Connection correspondiente —
nunca se escribe literal en el SQL.

### Lo que deliberadamente NO se construye todavía

Una **DAG factory** (generar DAGs desde YAML). Los pipelines comparten la forma
RAW→TRF→CONSUMPTION, pero abstraer antes de ver el patrón real produce una factory que pelea
contra cada caso especial. Se escriben los primeros 3 DAGs explícitos y se **extrae la factory
de lo que demostró ser común**, en fase 4. Si al tercero no hay patrón claro, no se hace.

### Acceso y secrets

- **UI:** subdominio con Caddy como reverse proxy y TLS automático de Let's Encrypt.
- **Llaves SA (3):** montadas como volumen desde `/opt/airflow/secrets/`, fuera de git,
  permisos `600`, una Airflow Connection por proyecto.
- **Tokens de APIs externas** (WMS, Wing, PrintFactory): Airflow Variables cifradas con Fernet key.
- **Webhook de Slack y credenciales SMTP:** Airflow Connections, no `.env` ni código.
- El `.env.example` se genera con las llaves vacías; el usuario las llena a mano.

### Backup y recuperación

Snapshot diario del droplet + `pg_dump` de la metadata a GCS. Si el droplet muere,
`bootstrap.sh` lo reconstruye desde cero. Los DAGs viven en git; lo único que se pierde es el
historial de corridas.

## 5. Plan de migración — strangler incremental

| Fase | Qué | Semana | Criterio de salida |
|---|---|---|---|
| **0** | Infra base + 3 Connections | 1 | Un DAG dummy corre programado y su fallo llega a Slack **y** correo |
| **1** | Piloto `in_out_analytics` | 2 | 5 corridas consecutivas con resultado idéntico a Composer |
| **2** | **Apagar Composer** | 3 | Factura de `b-materials` sin línea de Composer |
| **3** | Reportes `kpis_wms_batch` | 4 | `analisis_inventario_estancado` corre solo; `wing_heatmap` dispara al depositarse el Excel |
| **4** | Procesos programados `kpis_wms` (×8) | 5-6 | 8 Cloud Schedulers pausados, DAGs con dependencias reales |
| **5** | `media/data_lake_produccion` | 7-8 | Scheduler apagado, Service `media-dl-trf` apagado, fire-and-forget eliminados |

**Por qué este orden:** la fase 1 es el pipeline que ya está escrito — prueba la infra sin
escribir lógica nueva. La fase 2 captura el ahorro en la semana 3, antes de la parte difícil,
así el proyecto se paga solo aunque se detenga ahí. La fase 3 tiene riesgo cero y sirve de
prueba de carga temprana de los 4 GB. `media/data_lake_produccion` va al final por ser el
único con la cadena completa y el más crítico en producción.

### Fase 1 — precondición obligatoria

`terraform/main.tf:213` sube el DAG a Composer desde una ruta absoluta de la laptop de un
compañero (`C:/Users/Carlos Lopez/...`), no desde el repo. **Nada garantiza que la copia
versionada sea la que corre.** Hay además una contradicción sin resolver: `README.md:116`
dice *"runs at 06:00 on the 1st of the month"* mientras el código dice `'30 15 * * *'`.

La fase 1 arranca **exportando el DAG real del bucket de Composer** y comparándolo contra la
copia del repo. Lo que corre en producción es la fuente de verdad, no el archivo versionado.

### Shadow-run — cómo se hace sin duplicar escrituras

Correr Airflow y Composer en paralelo sobre el mismo Cloud Run Job significa **dos
ejecuciones escribiendo al mismo dataset**. Antes de activar el shadow en fase 1 hay que
verificar que el EL de `wms-in-out-analytics` sea idempotente. Dos opciones:

- **Si es idempotente:** shadow directo sobre el mismo destino, comparando resultados.
- **Si no lo es:** el shadow corre contra un dataset espejo (`*_shadow`), y la comparación
  es entre datasets, no entre corridas.

La verificación es tarea de fase 1, previa a activar nada.

### Fase 3 — lo que sí y lo que no se puede automatizar

`wing_heatmap/CLAUDE.md:17` dice *"Descargar el Excel desde el portal Wing"*. Ese reporte
**no es automatizable de punta a punta** sin una API de Wing que hoy no existe. El alcance
real: un bucket de staging en GCS donde el analista deposita el Excel, y un DAG que dispara
al detectarlo. Se elimina el trabajo de correr el script, no el de conseguir el archivo.

`analisis_inventario_estancado` sí es desatendido completo.

### Regla de rollback

Cada Cloud Scheduler que se apaga queda **pausado, no borrado, durante 30 días**. Volver
atrás es despausarlo — un click, cero despliegue.

### Limpieza en fase 2

El terraform de Composer se elimina junto con el ambiente, incluida la ruta absoluta de
`main.tf:213`. Mientras ese recurso exista, un `terraform apply` de cualquiera vuelve a
prender la factura.

## 6. Costos

**Este spec no declara cifras de costo.** La v1 afirmaba un ahorro concreto que no estaba
respaldado por nada del repo. Lo que sí está establecido:

- Composer `ENVIRONMENT_SIZE_SMALL` es, con diferencia, el gasto dominante del stack de
  orquestación, y es un costo **fijo mensual** independiente de cuánto se use.
- Un droplet de 4 GB con snapshots es un costo fijo **un orden de magnitud menor**.
- **No hay ahorro por apagar Cloud Run Services.** `media/data_lake_produccion/trf/cloudbuild.yaml:50`
  declara `--min-instances=0` y ningún otro `cloudbuild.yaml` declara `min-instances`
  (default 0). Todos escalan a cero: su costo en reposo es ~$0. La v1 afirmaba lo contrario.
- Los Cloud Run **Jobs** no generan ahorro — cobran solo por ejecución.

**Tarea bloqueante antes de presentar a dirección:** obtener de la facturación real de GCP el
costo mensual de Composer, y el pricing vigente de DigitalOcean. El caso de negocio se
construye con esos dos números, no con estimaciones.

**Hallazgo lateral, fuera de alcance:** `logistics/app_entradas_salidas/terraform/main.tf:37`
declara un VPC connector con `min_instances = 2` — costo fijo que nadie está revisando.
Pertenece al backend de la app, no a orquestación, pero vale levantarlo aparte.

## 7. Riesgos

| Riesgo | Mitigación |
|---|---|
| Tres llaves SA viviendo en DigitalOcean | SA por proyecto con roles mínimos, rotación a 90 días documentada, ticket de migración a GCE en el backlog |
| 4 GB se queda corto | la fase 3 lo detecta temprano y barato; pools de concurrencia limitados desde el día 1 |
| El DAG desplegado difiere del repo | precondición de fase 1: exportar del bucket, no asumir |
| Shadow-run duplicando escrituras | verificar idempotencia antes de activar; dataset espejo si no la hay |
| Apagar `kpis-wms-trf` por error | el Service permanece encendido por diseño (§3.5); documentado en el propio DAG de manifiestos |
| Alguien depende de Composer sin que lo sepamos | antes de apagar en fase 2, confirmar con clopez quién consume ese DAG |
| Droplet es punto único de falla | snapshot diario + `bootstrap.sh` reproducible |

## 8. Fuera de alcance

- **Webhook de WMS con Cloud Tasks** — se queda event-driven. Airflow orquesta tiempo, no
  eventos. El Service `kpis-wms-trf` que lo atiende permanece encendido (§3.5).
- **`last_mile_prediction_wms`** — trimestral, con optimización de rutas contra la API de
  Google Maps. No cumple el umbral de la regla 4 (§3.3) y su frecuencia no justifica un DAG.
- **`data_lake_campanas_faltantes`** — marcado "Integración pendiente" en `AGENTS.md`; su
  código vive en otra rama. Fuera hasta que se reactive.
- **Migración a GCE** — backlog, con ticket.
- **Subir a Airflow 3.x** — fase posterior, con su propia validación.

**Deuda que el proyecto retira:** los scripts `kpis_wms_batch/scripts/batch_*.py` existen para
re-correr a mano cuando un Scheduler falla. Con Airflow, re-ejecutar es un click en el UI.
Se retiran conforme cada pipeline migra.

## 9. Traza de auditoría

La v1 de este documento fue auditada dos veces de forma independiente el 2026-09-23:
por Claude contra el repo, y por un minero (`agy` / `gemini-3.8-flash-high`) bajo el
protocolo de `buho_data_pipelines/standards/DELEGACION.md` §4 L1 — worktree desechable, gate de escritura
verificado, 3 de 3 afirmaciones muestreadas contra el código.

15 hallazgos distintos. Los dos críticos:

1. **§3.5 contradecía a §8.** La regla de "apagar el Service `trf/` correspondiente" habría
   tumbado el webhook de ShipStream, que el mismo documento declaraba fuera de alcance.
2. **§2 colapsaba `etl_sheets` en un solo trigger semanal.** Migrarlo así habría congelado
   la carga diaria de tiempos.

Ambos eran errores de generalizar sin verificar: reglas escritas en singular sobre
infraestructura que no era separable. El report completo no se versiona (`buho_data_pipelines/standards/DELEGACION.md` §2).
