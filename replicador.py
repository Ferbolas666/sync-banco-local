import time
import fdb
import os
import json
import requests
import traceback
from requests.exceptions import RequestException

def ler_connection_txt(caminho_txt):
    config = {}
    try:
        with open(caminho_txt, 'r', encoding='utf-8') as f:
            for linha in f:
                if '=' in linha:
                    chave, valor = linha.strip().split('=', 1)
                    config[chave.strip()] = valor.strip()
        return config
    except Exception as e:
        print(f"❌ Erro ao ler arquivo de configuração: {e}")
        return {}

def parse_dados(dados_str):
    if not dados_str:
        return {}
    
    # Se já for um dicionário
    if isinstance(dados_str, dict):
        return dados_str
    
    # Se for string no formato "chave: valor, chave: valor"
    if isinstance(dados_str, str) and ':' in dados_str:
        dados_dict = {}
        partes = [p.strip() for p in dados_str.split(',') if ':' in p]
        for parte in partes:
            try:
                chave, valor = [x.strip() for x in parte.split(':', 1)]
                # Converter strings vazias para None
                valor = None if valor == '' else valor
                dados_dict[chave] = valor
            except ValueError:
                continue
        return dados_dict
    
    # Se for JSON válido
    try:
        return json.loads(dados_str)
    except json.JSONDecodeError:
        return {"raw": dados_str}  # Fallback

def converter_valores(dados_dict):
    dados_ajustados = {}
    for key, value in dados_dict.items():
        if value is None:
            dados_ajustados[key] = None
        elif isinstance(value, str):
            if value.isdigit():
                # Converter para inteiro
                dados_ajustados[key] = int(value)
            elif value.replace('.', '', 1).isdigit() and value.count('.') == 1:
                # Converter para float
                dados_ajustados[key] = float(value)
            elif value.lower() in ['true', 'false']:
                # Converter para booleano
                dados_ajustados[key] = value.lower() == 'true'
            else:
                dados_ajustados[key] = value
        else:
            dados_ajustados[key] = value
    return dados_ajustados

def monitorar_logs(dsn_local, user_local, pass_local, api_url, intervalo_segundos=1):
    print("✅ Monitorando LOG_ALTERACOES e enviando dados para API remota...")
    print(f"Configurações: DSN={dsn_local}, User={user_local}, API={api_url}")
    
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    last_id_log = 0

    while True:
        try:
            # Conexão com o Firebird (com reconexão automática)
            print("🔁 Tentando conectar ao banco local...")
            conn_local = fdb.connect(dsn=dsn_local, user=user_local, password=pass_local)
            cur_local = conn_local.cursor()
            print("✅ Conectado ao banco local com sucesso!")

            # Consulta para buscar novos registros
            cur_local.execute("""
                SELECT ID, TABELA, OPERACAO, DADOS, DATA_ALTERACAO, ID_REGISTRO
                FROM LOG_ALTERACOES
                WHERE ID > ?
                ORDER BY ID
            """, (last_id_log,))

            rows = cur_local.fetchall()
            print(f"🔍 Encontrados {len(rows)} registros novos")

            if not rows:
                print("⏳ Nenhum novo registro encontrado, aguardando...")
            
            for id_log, tabela, operacao, dados, data_alteracao, id_registro in rows:
                log_msg = f"[{data_alteracao}] {operacao} em {tabela}, ID_LOG={id_log}, ID_REGISTRO={id_registro}"
                print(log_msg)

                # Processa os dados
                dados_dict = parse_dados(dados)
                dados_ajustados = converter_valores(dados_dict)
                print(f"📊 Dados processados: {dados_ajustados}")

                # Preparar payload para a API
                payload = {
                    "id_log": id_log,
                    "tabela": tabela,
                    "operacao": operacao,
                    "dados": dados_ajustados,
                    "id_registro": id_registro,
                    "data_alteracao": str(data_alteracao)
                }

                try:
                    print(f"🚀 Enviando dados para API: {api_url}")
                    response = requests.post(
                        api_url,
                        json=payload,
                        headers=headers,
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        print(f"✅ Dados enviados com sucesso! Response: {response.text}")
                    else:
                        print(f"⚠️ Resposta inesperada da API: {response.status_code} - {response.text}")
                    
                    response.raise_for_status()
                    
                except RequestException as e:
                    print(f"❌ Erro ao enviar dados para a API: {str(e)}")
                    if hasattr(e, 'response') and e.response:
                        print(f"Detalhes do erro: {e.response.text}")

                # Atualizar o último ID processado
                last_id_log = id_log

            # Fechar conexão e cursor
            cur_local.close()
            conn_local.close()
            print("🔒 Conexão com banco local fechada")

        except fdb.Error as e:
            print(f"🔥 Erro no Firebird: {e}")
            print(traceback.format_exc())
            time.sleep(5)  # Espera antes de tentar reconectar
        except KeyboardInterrupt:
            print("⏹️ Monitoramento interrompido manualmente.")
            break
        except Exception as e:
            print(f"💥 Erro inesperado: {e}")
            print(traceback.format_exc())
            time.sleep(5)

        print(f"⏱️ Aguardando {intervalo_segundos} segundos...")
        time.sleep(intervalo_segundos)

if __name__ == "__main__":
    caminho_txt = r"C:\CONEXAO\Connection.txt"

    if not os.path.exists(caminho_txt):
        print(f"❌ Arquivo de configuração não encontrado: {caminho_txt}")
        exit(1)

    config = ler_connection_txt(caminho_txt)
    
    # Verificar se as configurações necessárias foram carregadas
    required_keys = ['DSN_LOCAL', 'USER_LOCAL', 'PASS_LOCAL']
    missing_keys = [key for key in required_keys if key not in config]
    
    if missing_keys:
        print(f"❌ Chaves ausentes no arquivo de configuração: {', '.join(missing_keys)}")
        exit(1)

    # URL para acessar a API
    #api_url = "http://localhost:8000/replicar"  # Fora do Docker
    api_url = "http://api-replicador:8000/replicar"  # Dentro da rede Docker

    print("⚙️ Configurações carregadas:")
    print(f" - DSN_LOCAL: {config['DSN_LOCAL']}")
    print(f" - USER_LOCAL: {config['USER_LOCAL']}")
    print(f" - API_URL: {api_url}")

    monitorar_logs(
        dsn_local=config['DSN_LOCAL'],
        user_local=config['USER_LOCAL'],
        pass_local=config['PASS_LOCAL'],
        api_url=api_url,
        intervalo_segundos=1
    )