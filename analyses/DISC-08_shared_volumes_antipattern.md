# DISC-08: Sdilene volumes — proc je to antipattern pro Edge Worker

## Zdroj

- https://medium.com/apache-airflow/shared-volumes-in-airflow-the-good-the-bad-and-the-ugly-22e9f681afca (Jarek Potiuk, Airflow PMC)

## Relevance: STREDNI (dulezite jako anti-pattern)

## Souhrn

Clanek od Airflow PMC clena vysvetluje, proc sdilene volumes (NFS/EFS/SMB) jsou problematicke pro distribuovane Airflow nasazeni. Puvodne o DAG distribuci, ale principy plati i pro datovy transfer.

## Klicove poznatky

### "The Good" (male instalace)

- Jednoduchost: "drop files in a folder"
- OK pro male instalace s par DAGy a jednim uzivatelem
- Zadne nove nastroje

### "The Bad" (stredni instalace)

- Zadna version control, zadne sledovani zmen
- Zadna conflict resolution pri soucasne editaci
- S rostem tymu a poctu DAGu = prekazka

### "The Ugly" (seriozni problemy)

1. **Performance**: NFS/EFS = iluze lokalniho FS. Ve skutecnosti kazda operace = sitovy RPC call
2. **Scheduler hammering**: Airflow scheduler neustale skenuje DAG folder → zaplavi NFS requesty
3. **Zadne atomicke updaty**: soubory na NFS nejsou konzistentni ve stejnem case (casti noveho kodu, casti stareho → import errors)
4. **Cache eviction**: lokalni NFS cache se vyprazni s rostem poctu souboru → continuous re-download
5. **Cost explosion**: vice IOPS = vyssi naklady, neocekavane

### Relevance pro Edge Worker datovy transfer

Sdileny volume mezi centralni Airflow instanci a Edge Workerem na vyrobni lince:
- **Jina sit** (factory floor ↔ server room) → vysoka latence, nespolehlivost
- **NFS pres WAN/VPN** = velmi pomale
- **Zadna atomicita** = edge zapise soubor, central ho cte pred dokoncenim
- **SMB omezeni**: neumi chmod/chown, Airflow ocekava POSIX FS

### Zaver autora

Object storage (S3/MinIO) je spravny pristup pro distribuovane prostredi. Git-sync pro DAGy, object storage pro data.
