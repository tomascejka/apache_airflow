# ANA-02: Automotive ETL - Vstupni zadani

Status: VSTUPNI ZADANI pro technickou analyzu realizovatelnosti

## Domena

Automotive - vyrobni linka

## Architektura prostredi

```
LINKA
  |
  +-- Stroj 1
  |     +-- Device 1a
  |     +-- Device 1b
  |
  +-- Stroj 2
  |     +-- Device 2a
  |
  +-- Stroj N
        +-- Device Na
        +-- Device Nb
        +-- Device Nc
```

- Linka ma N stroju
- Stroj muze mit N devices
- V lince je computer/cluster pocitacu s **Windows OS**
- Na tomto pocitaci se sbiraji informace z machines/devices
- Data jsou **ruzne strukturovana** (lisi se per machine/stroj)
- Data se ukladaji na **filesystem Windows OS**

## Datovy tok (high level)

```
[Stroje/Devices] --> [Windows PC v lince] --> [Filesystem] --> [Batch zpracovani]
```

## Co je zname

- Vstup: soubory na Windows filesystem (ruzne struktury per stroj)
- Zpracovani: batch (cyklicke/planovane)
- Vystup: zatim nespecifikovany

## Co neni zname

- Formaty souboru (CSV? XML? JSON? binarne? proprietarni?)
- Frekvence vzniku dat (kazdy cyklus stroje? kazdou minutu? hodinu?)
- Objem dat (KB? MB? GB za den?)
- Kam data pujdou po zpracovani (databaze? cloud? jiny system?)
- Pozadavky na latenci (je ok zpracovat za hodinu? nebo do minuty?)
- Sitova topologie (je Windows PC pristupny ze site? VPN? izolovan?)

## Dalsi krok

Technicka analyza realizovatelnosti - viz ANA-03 (pripravit)
