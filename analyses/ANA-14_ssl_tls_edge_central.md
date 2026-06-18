# ANA-14: SSL/TLS pro edge-to-central komunikaci

## Kontext

Edge Worker komunikuje s centralou pres HTTP. V produkci jde o bezpecnostni riziko — data (vcetne JWT tokenu a ETL dat) jdou po siti bez sifrovani. Tato analyza resi jak zabezpecit komunikaci.

Discovery zdroje: [DISC-11](DISC-11_tls_airflow_edge.md), [DISC-12](DISC-12_tls_on_premise_certifikaty.md)

## Co je treba sifrovane

| Komunikace | Protokol | Data na drate | Riziko bez TLS |
|------------|----------|---------------|----------------|
| Edge → Airflow API | HTTP | JWT token, heartbeaty, task status | Odposlech JWT → pristup k cele API |
| Edge → SeaweedFS | HTTP (S3) | S3 access key, ETL data | Odposlech klice → pristup k datum |
| Browser → Airflow UI | HTTP | Session cookie, heslo | Odposlech session → pristup k UI |
| Airflow → PostgreSQL | TCP | DB credentials, queries | Odposlech → pristup k metadata DB |

**Priorita**: edge ↔ central (jde pres factory sit), ostatni jsou interni na serveru.

## Varianty

### Varianta A: Nginx reverse proxy s TLS terminaci (DOPORUCENO)

**Princip**: jeden nginx na centralnim serveru, TLS terminace na nem. Backend sluzby (Airflow, SeaweedFS) komunikuji intern v plain HTTP.

```
EDGE WORKER (linka)                    CENTRAL SERVER
                                ┌──────────────────────────┐
  ──── HTTPS (port 443) ────>  │  Nginx (TLS terminace)   │
                                │   ├── /airflow/* → :8080  │
                                │   ├── /edge/*   → :8080  │
                                │   └── /s3/*     → :8333  │
                                │                          │
                                │  Airflow (:8080) ←HTTP   │
                                │  SeaweedFS (:8333) ←HTTP │
                                │  PostgreSQL (:5432)      │
                                └──────────────────────────┘
```

**Nginx konfigurace (priklad):**

```nginx
server {
    listen 443 ssl;
    server_name airflow.factory.local;

    ssl_certificate     /etc/nginx/certs/server.crt;
    ssl_certificate_key /etc/nginx/certs/server.key;
    ssl_protocols       TLSv1.2 TLSv1.3;

    # Airflow UI + API + Edge Worker API
    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # SeaweedFS S3 API
    location /s3/ {
        proxy_pass http://localhost:8333/;
        proxy_set_header Host $host;
    }
}
```

**Edge Worker konfigurace:**
```
AIRFLOW__EDGE__API_URL=https://airflow.factory.local/edge_worker/v1/rpcapi
```

**SeaweedFS (XCom backend) connection:**
```
endpoint_url=https://airflow.factory.local/s3
```

**Vyhody:**
- Jeden bod pro TLS (jeden certifikat, jedno misto pro renewal)
- Backend sluzby se nemeni (plain HTTP intern)
- Zadne problemy s interni komunikaci (scheduler ↔ API zustava HTTP)
- Nginx zvlada vysoke zatizeni

**Nevyhody:**
- Dalsi komponenta (nginx kontejner)
- Konfigurace reverse proxy (jednorazova)

### Varianta B: TLS primo na Airflow API serveru

**Princip**: Airflow sam terminuje TLS.

```
AIRFLOW__API__SSL_CERT=/certs/server.crt
AIRFLOW__API__SSL_KEY=/certs/server.key
```

**Nevyhody:**
- Interni komunikace (scheduler → API) taky pouziva HTTPS → problemy (issue #55147)
- Kazda sluzba (Airflow, SeaweedFS) potrebuje vlastni TLS konfiguraci
- Slozitejsi sprava certifikatu

**Nedoporuceno pro produkci.**

### Varianta C: VPN (site-to-site)

**Princip**: factory sit ↔ serverovna propojeny pres VPN tunel. Vsechna komunikace sifrovana na sitove urovni.

**Vyhody:**
- Zadna zmena v aplikacich (HTTP zustava HTTP)
- Sifrovany cely provoz (ne jen Airflow)

**Nevyhody:**
- Vyzaduje VPN infrastrukturu (HW/SW)
- Komplexnejsi sitova konfigurace
- Dalsi SPOF (VPN gateway)
- Muze byt overkill pokud jde jen o Airflow komunikaci

**Vhodne** pokud zakaznik uz ma VPN infrastrukturu.

## Certifikaty pro on-premise

| Typ | Cena | Automaticke renewal | Pro factory floor |
|-----|------|--------------------|--------------------|
| **Self-signed** | Zdarma | Ne (manualni, ~1 rok) | Nejjednodussi start |
| **Interni CA** | Zdarma (vlastni infra) | Mozne (step-ca, cfssl) | Pro produkci pokud ma zakaznik PKI |
| **Let's Encrypt** | Zdarma | Ano (certbot) | **Ne** — vyzaduje verejnou domenu |
| **Komercni** | Placene | Ano | Prebytecne pro interni sit |

### Pro PoC: Self-signed certifikat

```bash
# Generovat self-signed cert (platny 1 rok)
openssl req -x509 -nodes -days 365 \
  -newkey rsa:2048 \
  -keyout server.key \
  -out server.crt \
  -subj "/CN=airflow.factory.local" \
  -addext "subjectAltName=DNS:airflow.factory.local,IP:192.168.1.100"
```

Edge worker musi mit `server.crt` v trust store, nebo:
```
# Preskocit verifikaci (JEN pro PoC, NE produkce!)
export CURL_CA_BUNDLE=""
# nebo v Python: requests.get(..., verify=False)
```

### Pro produkci: Interni CA

1. Nasadit interni CA (napr. `step-ca` — lightweight, open-source)
2. Vydat server certifikat pro centralu
3. Distribuovat root CA cert na vsechny edge workery
4. Automaticke renewal (step-ca umí ACME protocol i pro interni sit)

## Architektura s TLS

```
┌─────────────────────────────────────────────────────┐
│ CENTRAL SERVER (Linux)                              │
│                                                     │
│  ┌──────────────────────────┐                       │
│  │ Nginx (port 443, TLS)    │ ← jediny vstupni bod │
│  │  ssl_cert: server.crt    │                       │
│  └──────┬──────┬────────────┘                       │
│         │      │                                    │
│    HTTP │ HTTP │                                    │
│         ▼      ▼                                    │
│  ┌──────────┐ ┌────────────┐                        │
│  │ Airflow  │ │ SeaweedFS  │                        │
│  │ :8080    │ │ :8333      │                        │
│  └──────────┘ └────────────┘                        │
│                                                     │
│  PostgreSQL :5432 (jen lokalne, neni exponovano)     │
└─────────────────────────────────────────────────────┘
         ▲                    ▲
         │ HTTPS              │ HTTPS
         │ (port 443)         │ (port 443)
┌────────┴───────┐   ┌───────┴────────┐
│ Edge Worker 1  │   │ Edge Worker N  │
│ (linka 1)      │   │ (linka N)      │
└────────────────┘   └────────────────┘
```

## Doporuceni

### Pro PoC / testovani

- Self-signed certifikat + nginx reverse proxy
- Edge worker: `verify=False` nebo import self-signed cert

### Pro produkci

1. **Nginx reverse proxy** s TLS terminaci (Varianta A)
2. **Interni CA** (step-ca) pro automaticke vydavani certifikatu
3. Root CA cert distribuovany na vsechny edge workery
4. PostgreSQL: `sslmode=require` pro DB komunikaci

### Co je treba zmenit oproti soucasnemu setupu

| Zmena | Kde | Slozitost |
|-------|-----|-----------|
| Pridat nginx kontejner | docker-compose.yaml | Nizka |
| Vygenerovat certifikat | Jednorazove | Nizka |
| Zmenit api_url na HTTPS | Edge worker env | Nizka |
| Zmenit S3 endpoint na HTTPS | Airflow connection | Nizka |
| Airflow `enable_proxy_fix = True` | airflow.cfg | Nizka |

**Celkova slozitost: NIZKA** — jde o jednorazovy setup nginx + certifikat.

## Souvisejici analyzy

- [ANA-11](ANA-11_edge_worker_windows_deployment.md) — edge worker deployment (sitove pozadavky)
- [ANA-12a](ANA-12a_object_storage_analyza.md) — SeaweedFS bezpecnost (S3 keys, TLS)
