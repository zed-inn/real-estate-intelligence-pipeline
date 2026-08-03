FROM python:3.12-slim-bookworm

RUN pip install uv

ENV UV_LINK_MODE=copy
ENV UV_COMPILE_BYTECODE=1

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --no-cache

COPY . .

ENV PYTHONPATH=/app
ENV PATH="/app/.venv/bin:$PATH"
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('BAAI/bge-small-en-v1.5')"

ENV HF_HUB_OFFLINE=1
CMD ["python", "-m", "src.main"]
