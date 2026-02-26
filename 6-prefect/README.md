# Prefect Classroom Demo (Two Stages)

This repo is set up for a two-stage classroom walkthrough:

1. Stage 1: run a flow manually (no deployment)
2. Stage 2: run a minimal deployment from `prefect.yaml`

## Prerequisites

- Docker Desktop (or Docker Engine + Compose)
- Ports `4200` and `8081` available on your machine
- `.env` present in the project root

## Stage 1: Manual Flow Run (No Deployment)

Goal: prove the Prefect environment is running and that flow runs show up in the UI.

### Start Prefect Server

```bash
docker compose --profile stage1 up -d prefect-server
```

Open the UI:

- `http://localhost:4200`

### Run `flow.py` manually in the CLI container

```bash
docker compose --profile stage1 run --rm prefect-cli bash
```

Inside the container:

```bash
uv sync --no-dev
uv run python flows/flow.py
```

### Validate

- In the UI, open **Flow Runs** and confirm a run for `greetings flow` appears.
- No deployment is required for this stage.

## Stage 2: Minimal Deployment via `prefect.yaml`

Goal: show how deployments add UI-triggered orchestration with worker execution.

### What this stage uses

- One deployment defined in `prefect.yaml`
- Entrypoint: `flows/01-ETLWeatherPrint.py:etl_weather_print`
- Work pool: `is566-pool`

### Start deployment services

```bash
docker compose --profile stage2 up -d prefect-server prefect-bootstrap prefect-worker
```

### Check bootstrap logs

```bash
docker compose logs -f prefect-bootstrap
```

Look for successful work-pool creation/inspection and deployment registration.

### Validate in UI

- Open **Deployments** and confirm exactly one deployment: `etl-weather-print-minimal`.
- Trigger a run from the deployment page.
- Confirm the worker picks up the run and it completes.

## Optional: Postgres Services (Not Needed for This Demo)

If you later want to demonstrate Postgres ETL flows:

```bash
docker compose --profile db up -d postgres adminer
```

Adminer UI: `http://localhost:8081`

## Negative-Path Teaching Check

To demonstrate configuration failure handling in Stage 2:

1. Temporarily blank `WEATHER_API_KEY` in `.env`
2. Trigger the deployment from UI
3. Show the run failure message and explain why restoring `WEATHER_API_KEY` fixes it

## Stop Everything

```bash
docker compose down
```
