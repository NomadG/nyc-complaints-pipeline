# 1. Use an official Python base image
# Use -slim or -alpine versions for smaller image sizes
FROM python:3.11-slim

# 2. Set the working directory in the container
WORKDIR /app

# 3. Installing UV in this 
COPY --from=docker.io/astral/uv:latest /uv /uvx /bin/

# 4. Copy the pyproject.toml and uv.lock files to the working directory
COPY pyproject.toml uv.lock ./

# 5. Install the dependencies using UV
RUN uv sync --no-dev

# finally copy the main ingestion script to the container
COPY main.py ./

# running the main.py script when the container starts
ENTRYPOINT [ "uv", "run", "python", "main.py" ]