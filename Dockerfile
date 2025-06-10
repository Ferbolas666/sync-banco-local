# Use uma imagem base com Python
FROM python:3.10-slim

# Definir diretório de trabalho
WORKDIR /app

# Copiar arquivos necessários
COPY . /app

# Instalar dependências
RUN pip install --no-cache-dir firebird-driver requests

# Instalar pacote fdb (driver Firebird)
RUN pip install fdb

# Criar diretório para configuração
RUN mkdir -p /CONEXAO

# Comando para executar o script
CMD ["python", "sync-banco-remoto.py"]