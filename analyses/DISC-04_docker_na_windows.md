# DISC-04: Docker na Windows pro Edge Worker

## Zdroje

- https://dev.to/_d7eb1c1703182e3ce1782/best-docker-desktop-alternatives-in-2025-rancher-podman-orbstack-and-more-3n2c
- https://www.devtoolreviews.com/reviews/docker-desktop-vs-podman-vs-rancher-desktop
- https://ajmani.dev/docker-desktop-alternatives-for-windows/
- https://docs.docker.com/desktop/setup/install/windows-install/

## Relevance: VYSOKA

## Souhrn

Docker na Windows bezi Linux kontejnery pres Hyper-V/WSL2 backend. Hlavni otazka: Docker Desktop licencovani vs free alternativy.

## Klicove poznatky

### Docker Desktop

- **Licencovani**: od 2022 placeny pro firmy s >250 zamestnancu nebo >$10M obratem
- **Cena**: Pro plan = $11/mesic/uzivatel (~$132/rok)
- **Vyhody**: GUI, auto-start, compose integrace, stabilni
- **Nevyhody**: licence, resource overhead (~2GB RAM pro VM)
- **POZOR**: nepodporuje Windows Server (jen Pro/Enterprise/Education)

### Rancher Desktop (FREE)

- Apache 2.0 licence — free pro vsechny, zadny employee threshold
- Pouziva containerd nebo Moby (dockerd) runtime
- GUI, podporuje docker-compose
- Kubernetes integrace (k3s)
- Windows/Mac/Linux

### Podman Desktop (FREE)

- Red Hat, daemonless, rootless
- GUI wrapper kolem Podman engine
- `podman-compose` nebo kompatibilita s docker-compose
- Windows/Mac/Linux
- Trochu mene user-friendly nez Docker Desktop

### Pro nas use case (Edge Worker kontejner)

Scenar: beh jednoho Linux kontejneru s edge workerem na Windows PC na lince.

| Kritérium | Docker Desktop | Rancher Desktop | Podman Desktop |
|-----------|---------------|-----------------|----------------|
| Licence | Placena (>250 emp) | Free (Apache 2.0) | Free (Apache 2.0) |
| Auto-start | Ano | Ano | Ano |
| Compose | Nativni | Ano (moby/nerdctl) | podman-compose |
| Stabilita | Nejvyssi | Vysoka | Stredni na Win |
| RAM overhead | ~2GB | ~2GB | ~2GB |
| Windows Server | Ne | Ne | Omezena |

### Jak by vypadal edge kontejner

```dockerfile
FROM python:3.12-slim
RUN pip install apache-airflow-providers-edge3
COPY dags/ /opt/airflow/dags/
COPY start_edge.sh /start_edge.sh
CMD ["/start_edge.sh"]
```

- Kontejner = Linux prostredi, zadne Windows path issues
- Dvojtecky v nazvech souboru = zadny problem (Linux FS)
- Task SDK funguje normalne (Linux)
- Pristup k datum ze stroje: volume mount z Windows FS (`-v C:\data:/data`)
