# DISC-03: WSL2 jako deployment platforma pro Edge Worker

## Zdroje

- https://github.com/microsoft/WSL/discussions/14261 — WSL2 in production on Windows Server 2025
- https://www.cod3r.com/2025/05/wsl2-start-on-boot/ — WSL2 auto-start na boot
- https://github.com/microsoft/WSL/issues/7365 — Start WSL2 on boot without login
- https://learn.microsoft.com/en-us/windows/wsl/ — Oficialni WSL docs

## Relevance: STREDNI

## Souhrn

WSL2 umoznuje beh Linux prostredi na Windows. Pro produkci je klicovy problem: auto-start bez loginu uzivatele a stabilita pri restartech.

## Klicove poznatky

### WSL2 = Hyper-V lightweight VM

- Bezi skutecny Linux kernel (ne emulace)
- Plna podpora systemd (od Windows 22H2)
- Vykon: nativni Linux vykon pro CPU-bound ulohy
- Filesystem: Linux FS je rychly, ale pristup k Windows FS (`/mnt/c/`) je pomalejsi

### Auto-start bez loginu

**Problem**: WSL2 standardne nestartuje dokud se uzivatel neprihlasi. Pro factory floor = kriticke (stroj se restartuje, nikdo se neprihlasuje).

**Reseni**:

1. **NSSM (Non-Sucking Service Manager)**: wrappuje WSL jako Windows Service
   - `nssm install AirflowEdge wsl.exe -d Ubuntu -- airflow edge worker`
   - Nastavi se na Auto start
   - Bezi pred loginem uzivatele
   - Obchazi Task Scheduler quirks

2. **Task Scheduler s boot triggerem**:
   - Trigger: "At system startup"
   - Action: `wsl.exe -d Ubuntu -- /path/to/start_edge.sh`
   - Slabsi nez NSSM (zavisi na login session)

3. **Windows .wslconfig**:
   ```ini
   [boot]
   systemd=true
   command=/path/to/startup.sh
   ```
   - Spusti se pri prvnim startu WSL, ne pri bootu Windows

### Enterprise sprava

- Microsoft roadmap: Group Policy kontroly pro WSL
- Moznost omezit povolene distribuce
- Endpoint Manager deployment
- Security policies across organizaci

### Omezeni pro produkci

- WSL2 neni oficalne "production-ready" — Microsoft rika, ze je to developer tool
- Pri neplanovanych restartech muze WSL2 prostredi neustartovat spravne
- Potreba monitorovat health WSL2 prostredi z Windows strany
- Networking: WSL2 ma vlastni virtual network adapter (NAT), ne bridged — komplikuje discovery z centraly
