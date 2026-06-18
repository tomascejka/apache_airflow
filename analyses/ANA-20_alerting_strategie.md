# ANA-20: Alerting strategie — Zabbix vs Prometheus AlertManager

## Kontext

Mame dva monitorovaci systemy (poc04: Prometheus+Grafana, poc05: Zabbix). Oba umí alertovat. Ktery pouzit, nebo oba? Jak nastavit eskalaci?

## Co chceme alertovat

### Infrastrukturni alerty (infra)

| Alert | Zavaznost | Zdroj metriky |
|-------|-----------|---------------|
| Scheduler nereaguje (heartbeat) | CRITICAL | Prometheus (airflow_scheduler_heartbeat) |
| Worker zatez > 90% | WARNING | Prometheus (executor_running_tasks / parallelism) |
| Disk plny > 85% | WARNING | Prometheus (node_exporter) / Zabbix agent |
| DB connections vycerpany | CRITICAL | Prometheus (pg_stat_activity) |
| Kontejner restartoval | WARNING | Prometheus (container_restarts) |

### Business alerty (provoz)

| Alert | Zavaznost | Zdroj |
|-------|-----------|-------|
| DAG selhal (task failed) | HIGH | Zabbix (REST API: /api/v2/dags/{dag_id}/dagRuns) |
| Import errors (vadny DAG) | WARNING | Zabbix (REST API: /api/v2/importErrors) |
| Airflow not healthy | DISASTER | Zabbix (/monitor/health) |
| Edge Worker offline | HIGH | Zabbix (REST API: edge worker status) |
| ETL data nesla (SLA miss) | HIGH | Airflow callback / Zabbix |

## Srovnani

| Kriterium | Zabbix | Prometheus AlertManager |
|-----------|--------|----------------------|
| **Konfigurace** | GUI (web UI) | YAML soubory (kod) |
| **Alert pravidla** | Trigger expressions (jednoduche) | PromQL (mocne, slozitejsi) |
| **Eskalace** | Built-in (step 1→2→3, casove) | Nutne external (PagerDuty, OpsGenie) |
| **Notifikace** | Email, SMS, Slack, webhook, script | Email, Slack, webhook, PagerDuty |
| **Skupiny** | User groups + media types | Routes + receivers |
| **Maintenance mode** | Built-in (GUI) | Silences (API/UI) |
| **Acknowledge** | Built-in (GUI, s komentarem) | Silences (neni prave acknowledge) |
| **HA** | Nativni (6.0+, active-standby) | Cluster mode (gossip protocol) |
| **Vhodne pro** | Business alerting, eskalace, ITSM | Infra metriky, dynamicke prostredi |

## Strategie

### Strategie 1: Zabbix pro vse (NEJJEDNODUSSI)

```
Zabbix → email/SMS/Slack
  ├── Infra: Zabbix agent na serverech + HTTP check
  └── Business: HTTP Agent → Airflow REST API
```

**Vyhody:**
- Jeden nastroj, jedna konfigurace
- Built-in eskalace (email → SMS → ticket)
- GUI — neni treba znat PromQL
- Pokud zakaznik uz Zabbix ma → nulova nova infrastruktura

**Nevyhody:**
- Zabbix neni idealni pro Prometheus metriky (StatsD pipeline)
- Trigger expressions mene mocne nez PromQL
- Bez Zabbix agenta na kontejnerech je infra monitoring omezeny

**Kdy**: zakaznik uz ma Zabbix, maly tym, jednoduchost prioritou.

### Strategie 2: AlertManager pro vse (INFRASTRUCTURE-AS-CODE)

```
Prometheus → AlertManager → email/Slack/PagerDuty
  ├── Infra: Prometheus metriky (StatsD, node_exporter)
  └── Business: Blackbox exporter → Airflow health check
```

**Vyhody:**
- Vsechen alerting v YAML (verzovatelne v Gitu)
- PromQL = mocny dotazovaci jazyk
- Nativni integrace s Prometheus metrikami

**Nevyhody:**
- Zadna built-in eskalace (nutny externi nastroj)
- Zadny acknowledge v UI (jen silences)
- Slozitejsi setup pro business alerty (Airflow REST API neni Prometheus metrika)
- Pokud zakaznik nema Prometheus → nova infrastruktura

**Kdy**: tym zna Prometheus, infrastructure-as-code kultura, uz bezi AlertManager.

### Strategie 3: Kombinace — Zabbix + AlertManager (DOPORUCENO)

```
Prometheus → AlertManager → infra alerty (email/Slack)
Zabbix → business alerty (email → SMS → ticket)
```

**Rozdeleni odpovednosti:**

| Typ | Nastroj | Proc |
|-----|---------|------|
| Infra metriky (CPU, RAM, disk, kontejnery) | **AlertManager** | Prometheus uz metriky sbira (poc04) |
| Airflow infra (scheduler heartbeat, executor load) | **AlertManager** | StatsD → Prometheus pipeline uz existuje |
| Business (DAG failed, import errors, health) | **Zabbix** | REST API polling + built-in eskalace (poc05) |
| Edge Worker status | **Zabbix** | HTTP Agent → API server |
| SLA misses | **Zabbix** | Airflow callback → Zabbix webhook |

**Vyhody:**
- Kazdy nastroj dela to, v cem je nejlepsi
- Uz mame oba nastavene (poc04 + poc05)
- Zabbix eskalace pro business (email → SMS → ticket) bez dalsich nastroju

**Nevyhody:**
- Dva systemy k udrzbe
- Alerty na dvou mistech (nutna dokumentace "kde co hledat")

## Eskalacni schema (priklad)

### Zabbix eskalace (business alerty)

```
Krok 1 (0 min):    Email → L1 support (dispece)
Krok 2 (15 min):   SMS → L1 support (pokud nepotvrzeno)
Krok 3 (30 min):   Email → L2 engineer
Krok 4 (60 min):   SMS → L2 engineer + email management
```

### AlertManager eskalace (infra alerty)

```yaml
# alertmanager.yml
route:
  receiver: 'email-l1'
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  routes:
    - match:
        severity: critical
      receiver: 'slack-oncall'
    - match:
        severity: warning
      receiver: 'email-l1'

receivers:
  - name: 'email-l1'
    email_configs:
      - to: 'support@firma.cz'
  - name: 'slack-oncall'
    slack_configs:
      - channel: '#airflow-alerts'
```

## Doporuceni

### Pokud zakaznik MA Zabbix

→ **Strategie 1 (Zabbix pro vse)** nebo **Strategie 3 (kombinace)**

Zabbix uz bezi, ma uzivatele, eskalace, media types. Pridat Airflow monitoring = par HTTP Agent itemu (viz poc05). Prometheus+Grafana pro dashboardy, AlertManager volitelne pro infra.

### Pokud zakaznik NEMA Zabbix

→ **Strategie 2 (AlertManager)** s Grafana OnCall pro eskalaci

Nebo nasadit Zabbix ciste pro Airflow business alerting (relativne jednoduchy setup).

### V obou pripadech

1. **Definovat severity levels** (co je CRITICAL vs WARNING vs INFO)
2. **Definovat eskalacni casy** (kolik minut pred eskalaci)
3. **Definovat prijemce** (kdo dostane jaky alert)
4. **Testovat alerty** — simulovat selhani, overit ze notifikace prijdou
5. **Dokumentovat** — "kde co hledat" guide pro on-call

## Otevrene otazky

| # | Otazka | Dopad |
|---|--------|-------|
| 1 | Ma zakaznik existujici Zabbix? | Urcuje strategii (1 vs 2 vs 3) |
| 2 | Ma zakaznik on-call proces? | Urcuje eskalacni schema |
| 3 | Jake komunikacni kanaly preferuje? (email, SMS, Slack, Teams) | Urcuje media types |

## Souvisejici analyzy

- [ANA-08](ANA-08_monitoring.md) — Prometheus + Grafana setup (poc04)
- [ANA-09](ANA-09_monitoring_zabbix.md) — Zabbix setup (poc05)
- [ANA-18](ANA-18_skalovani_edge_workeru.md) — Skalovani (infrastrukturni metriky)
