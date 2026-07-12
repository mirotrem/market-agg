FROM python:3.12-slim AS builder

WORKDIR /app
RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


FROM python:3.12-slim

RUN useradd --create-home --uid 1000 appuser
COPY --from=builder /venv /venv
ENV PATH="/venv/bin:$PATH"

WORKDIR /app
COPY app/ ./app/
COPY scripts/ ./scripts/

USER appuser
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
