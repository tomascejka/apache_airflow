# DISC-05: Prumyslove Linux mini PC pro factory floor

## Zdroje

- https://www.onlogic.com/store/computers/industrial/mini-pc/ — OnLogic industrial PCs
- https://www.sinsmarts.com/blog/best-linux-mini-pcs-in-2025/ — Best Linux Mini PCs 2025
- https://www.omgubuntu.co.uk/2024/07/radxa-x4-cheap-intel-n100-raspberry-pi-alternative — Radxa X4
- https://itsfoss.com/linux-based-mini-pc/ — Linux pre-installed PCs

## Relevance: STREDNI-VYSOKA

## Souhrn

Alternativa k Windows PC na lince: dedickovany mini Linux PC vedle existujiciho Windows stroje. Prumyslove varianty (fanless, dustproof) existuji v cenove kategorii $200-800.

## Klicove poznatky

### Prumyslove mini PC (factory-grade)

**OnLogic CL100 / CL200**:
- Fanless, dustproof, sirsi rozsah teplot (-20 az +60°C)
- Ubuntu pre-instalovany
- Intel Celeron/Atom, 4-8GB RAM, 64-256GB SSD
- Multiple Ethernet, serial porty (RS-232/RS-485), GPIO
- Cena: ~$400-800
- Urceno primo pro edge computing na factory floor

**Vlastnosti prumyslovych PC**:
- DIN rail mount (standardni montaz v rozvodne)
- Sirsi teplotni rozsah nez consumer hw
- Vibration resistance
- Delsi lifecycle (5-10 let dostupnost)

### Consumer mini PC (levnejsi alternativa)

**Intel NUC / ASUS NUC**:
- Intel N100/i3/i5, 8-16GB RAM, 256GB+ SSD
- Cena: $150-400
- Linux instalace bez problemu
- Kompaktni (10x10cm), tichy
- ALE: neni dustproof, omezeny teplotni rozsah

**Radxa X4**:
- Intel N100, $60
- Raspberry Pi form factor, ale x86 architektura
- Linux nativne
- Ethernet + WiFi
- Velmi levne, ale consumer kvalita

### Raspberry Pi 5

- ARM architektura — vetsina Airflow provideru funguje, ale nektere pip balicky nemusi byt dostupne
- 8GB RAM, $80
- Nizky prikon (~5W)
- **Problem**: ARM + Airflow = netestovano, mozne compile issues s nekterymi zavislostmi

### Pro nas use case

Edge Worker potrebuje:
- Python 3.10+ (pip install edge3)
- Sit k centrale (HTTP/HTTPS)
- Pristup k datum ze stroje (sit nebo USB/serial)
- ~2GB RAM (Python + edge worker)
- Minimalni disk (DAGy + logy)

→ I nejlevnejsi Linux mini PC (~$150) to splnuje. Prumyslovy PC za ~$500 pridava robustnost pro factory prostredi.
