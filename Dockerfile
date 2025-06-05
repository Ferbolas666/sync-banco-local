FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y libfbclient2 firebird-dev gcc && \
    pip install fdb && \
    apt-get clean

WORKDIR /app
COPY replicador.py .

CMD ["python", "replicador.py"]
