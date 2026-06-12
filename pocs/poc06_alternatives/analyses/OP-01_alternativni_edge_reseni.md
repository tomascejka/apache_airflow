# OP-01: Alternativni edge reseni pro Prefect/Dagster

## Otazka

Lze v Prefect nebo Dagster dosahnout ekvivalentu Airflow Edge Worker bez nativni podpory?

## Proc je dulezita

Pokud by existovalo jednoduche reseni pro edge execution v Prefect/Dagster, oslabilo by to hlavni argument pro Airflow v nasem use case.

## Mozne smery reseni

1. **Prefect Worker na remote stroji** — nainstalovat Prefect worker na linku, pripojit k Prefect serveru. Teoreticky mozne, ale neni to "edge worker" koncept — worker ocekava plny Python environment a sitove pripojeni k serveru.

2. **Dagster code location na remote stroji** — spustit Dagster code location na lince. Vyzaduje gRPC spojeni s Dagster daemon. Slozitejsi setup nez Airflow Edge Worker.

3. **SSH/Remote execution** — z centralniho orchestratoru spustit task pres SSH na remote stroji. Mozne ve vsech trech (Airflow SSHOperator, Prefect shell task, Dagster ssh resource). Ale neni to "agent" — je to push model, ne pull model.

4. **Message queue pattern** — orchestrator posle zpravu do fronty (RabbitMQ, Redis), worker na lince ji zpracuje. Custom reseni, neni nativni v zadnem orchestratoru.

## Co je treba zjistit

- Existuji community pluginy pro Prefect/Dagster edge execution?
- Je Prefect Worker na remote stroji stabilni pro long-running deployment?
- Jake jsou bezpecnostni implikace (firewall, VPN) kazdeho pristupu?
- Zkusenosti z praxe s distributed orchestraci v manufacturing?

## Status

OPEN — zatim neni treba resit (Airflow Edge Worker funguje a je overeny v poc02/poc03)
