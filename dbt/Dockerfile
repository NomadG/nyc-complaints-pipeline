FROM python:3.11-slim

WORKDIR /app

COPY --from=docker.io/astral/uv:latest /uv /uvx /bin/

COPY pyproject.toml uv.lock ./

RUN uv sync --no-dev

COPY nyc_complaints/ ./nyc_complaints/

ENV DBT_PROFILES_DIR=/app/nyc_complaints
ENV DBT_PROJECT_DIR=/app/nyc_complaints

RUN uv run dbt deps

ENTRYPOINT ["uv", "run", "dbt"]
