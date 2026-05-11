# Contrato del Proyecto — Signal Catcher

## Descripción

Sistema de tracking de anuncios en tiempo real para proyecto universitario.

## Integrantes y responsabilidades

| Integrante | Área principal | Componente |
|------------|---------------|------------|
| TBD        | Backend       | `api/`     |
| TBD        | Mensajería    | `consumer/aggregator.py`, `consumer/influx_writer.py` |
| TBD        | Almacenamiento | `storage/`, `consumer/storage_client.py` |
| TBD        | Observabilidad | `grafana/` |

## Acuerdos técnicos

- **Lenguaje:** Python 3.11
- **Estilo:** PEP 8. Sin comentarios obvios, sólo cuando el WHY no es evidente.
- **Variables de entorno:** Nunca hardcodear credenciales. Usar siempre el `.env`.
- **Git:** Feature branches + PR revisado por al menos un integrante antes de merge a `main`.
- **Commits:** Mensajes en inglés, en tiempo presente imperativo (`Add X`, `Fix Y`).

## Interfaces de contrato

### API → RabbitMQ

Los mensajes publicados deben incluir siempre:
- Todos los campos del modelo Pydantic correspondiente
- `event_type`: `"impression"` | `"click"` | `"conversion"`
- `received_at`: timestamp ISO 8601 UTC

### Consumer → InfluxDB

Measurement: `ad_events`

Tags: `event_type`, `state`, `advertiser_id`, `campaign_id`, `ad_id`, `search_keyword`

Fields: `count` (float), `revenue` (float), `ctr` (float), `conversion_rate` (float), `avg_time_to_click` (float), `avg_time_to_convert` (float)

### Consumer → MinIO

Path: `events/{event_type}/year=YYYY/month=MM/day=DD/hour=HH/{unix_ts}.json`

Contenido: array JSON con los eventos crudos del batch.

## Criterios de aceptación

- [ ] `docker compose up --build` levanta todos los servicios sin errores.
- [ ] POST `/api/events/impression` responde en < 50 ms bajo carga normal.
- [ ] Los 3 consumidores procesan mensajes en paralelo.
- [ ] Los eventos aparecen en el dashboard de Grafana en < 15 s tras el envío.
- [ ] Los archivos JSON crudos son accesibles en MinIO con la estructura de path acordada.
- [ ] Los mensajes malformados van al DLQ y no bloquean el pipeline.
