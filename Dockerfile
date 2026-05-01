FROM nedbank-de-challenge/base:1.0

WORKDIR /app

USER root

RUN apt-get update && apt-get install -y --no-install-recommends procps ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY jars/ /opt/delta/jars/

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline/ pipeline/
COPY config/ config/
COPY adr/ adr/

CMD ["python", "pipeline/run_all.py"]