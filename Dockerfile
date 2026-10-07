FROM python:3.14-slim AS prod

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY transaction_pipeline/ /build/transaction_pipeline/
COPY pyproject.toml requirements.txt /build/
RUN pip install --no-cache-dir -r /build/requirements.txt /build

RUN mkdir -p /state /config /secrets

CMD ["python", "-m", "transaction_pipeline"]

FROM prod AS dev
COPY requirements-dev.txt /tmp/reqs-dev.txt
RUN pip install --no-cache-dir -r /tmp/reqs-dev.txt
COPY tests/ /app/tests/
WORKDIR /app
ENTRYPOINT []
CMD ["pytest", "tests/"]
