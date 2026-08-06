FROM python:3.13-slim

# uv is distributed as a static binary; copying it beats installing via pip
COPY --from=ghcr.io/astral-sh/uv:0.9.7 /uv /uvx /bin/

WORKDIR /app

# Dependencies first, in their own layer. Editing source below does not
# invalidate this, so rebuilds skip the reinstall.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Now the source, and a second sync to install the package itself
COPY src/ ./src/
RUN uv sync --frozen --no-dev

# Put the venv's bin first so `python` resolves to the project interpreter
ENV PATH="/app/.venv/bin:$PATH"

# No CMD — docker-compose.yml supplies the command per service