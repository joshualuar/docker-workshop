FROM python:3.13.11-slim

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIr /code
ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./
RUN uv sync  --locked

RUN pip install pandas pyarrow

WORKDIR /code
# IMPORTANTE: Usar la ruta de la carpeta que vimos en tu captura
COPY pipeline/pipeline.py .

ENTRYPOINT [  "python", "pipeline.py" ] 
#"uv", "run",