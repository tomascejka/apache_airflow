# DISC-11: TLS/SSL pro Airflow a Edge Worker

## Zdroje

- https://airflow.apache.org/docs/apache-airflow/stable/howto/run-behind-proxy.html — Reverse proxy docs
- https://airflow.apache.org/docs/apache-airflow/stable/howto/run-with-self-signed-certificate.html — Self-signed cert
- https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/configurations-ref.html — Edge3 config
- https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/architecture.html — Edge architektura
- https://github.com/apache/airflow/issues/55147 — SSL issues s API serverem

## Relevance: VYSOKA

## Klicove poznatky

### Edge Worker podporuje HTTPS

- `AIRFLOW__EDGE__API_URL=https://hostname:port/edge_worker/v1/rpcapi`
- Edge worker nativne podporuje HTTPS v api_url
- Autentizace: JWT token (`AIRFLOW__API_AUTH__JWT_SECRET`)

### Airflow API server — TLS konfigurace

- `AIRFLOW__API__SSL_CERT` a `AIRFLOW__API__SSL_KEY` — primo na API serveru
- Self-signed: SAN musi obsahovat hostname (localhost, airflow-apiserver)
- Oficialne doporuceni: self-signed jen pro vyvoj, ne produkci

### Reverse proxy (doporuceno pro produkci)

- Nginx pred Airflow — TLS terminace na proxy, backend v plain HTTP
- Potrebne hlavicky: `X-Forwarded-For`, `X-Forwarded-Proto`, `Host`
- Airflow: `[webserver] enable_proxy_fix = True`
- Jedna konfigurace pro vsechny sluzby (Airflow UI + API + SeaweedFS)

### Znamy problem

- Issue #55147: DAGy/tasky selhavaji kdyz je corporate SSL cert na API serveru
- Pricina: interni komunikace (scheduler → API) taky pouziva HTTPS
- Reseni: reverse proxy (interni komunikace zustava HTTP, jen externi je HTTPS)
