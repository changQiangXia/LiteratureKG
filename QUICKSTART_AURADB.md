# AuraDB + Conda Quickstart

## 1) Create AuraDB instance

1. Sign in to Neo4j Aura and create a `Free` instance.
2. Save these values:
   - `URI` (looks like `neo4j+s://xxxx.databases.neo4j.io`)
   - `Username` (usually `neo4j`)
   - `Password` (you set this)
   - `Database` (usually `neo4j`)

## 2) Create and activate conda env

```powershell
conda create -n litkg python=3.11 -y
conda activate litkg
python -m pip install -r requirements.txt
```

## 3) Fill env vars

```powershell
Copy-Item .env.example .env
```

Edit `.env` and set:

```text
NEO4J_URI=neo4j+s://xxxx.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password
NEO4J_DATABASE=neo4j
```

## 4) Test connection

```powershell
python scripts/test_auradb_connection.py
```

Expected output:

```text
Connection OK
ok=1
server_time=...
```

## 5) Common failures

- `Missing required env var`: `.env` was not created or not filled.
- `Authentication failed`: username/password mismatch.
- `Unable to retrieve routing information`:
  - wrong URI, or
  - blocked outbound network.
  - verify URI starts with `neo4j+s://`.
