import time
import fdb
import os
import requests  # biblioteca para requisições HTTP

def ler_connection_txt(caminho_txt):
    config = {}
    with open(caminho_txt, 'r', encoding='utf-8') as f:
        for linha in f:
            if '=' in linha:
                chave, valor = linha.strip().split('=', 1)
                config[chave.strip()] = valor.strip()
    return config

def monitorar_logs(dsn_local, user_local, pass_local, api_url, intervalo_segundos=1):
    conn_local = fdb.connect(dsn=dsn_local, user=user_local, password=pass_local)
    cur_local = conn_local.cursor()

    last_id_log = 0
    print("✅ Monitorando LOG_ALTERACOES e enviando dados para API remota...")

    try:
        while True:
            cur_local.execute("""
                SELECT ID, TABELA, OPERACAO, DADOS, DATA_ALTERACAO
                FROM LOG_ALTERACOES
                WHERE ID > ?
                ORDER BY ID
            """, (last_id_log,))

            rows = cur_local.fetchall()

            for id_log, tabela, operacao, dados, data_alteracao in rows:
                print(f"[{data_alteracao}] {operacao} em {tabela}, ID={id_log}")
                print(f"DADOS: {dados}")

                payload = {
                    "id_log": id_log,
                    "tabela": tabela,
                    "operacao": operacao,
                    "dados": dados,
                    "data_alteracao": str(data_alteracao)
                }

                try:
                    response = requests.post(api_url, json=payload)
                    response.raise_for_status()
                    print(f"✅ Dados enviados com sucesso para a API: ID_LOG {id_log}\n")
                except requests.RequestException as e:
                    print(f"❌ Erro ao enviar dados para a API no ID_LOG {id_log}: {e}\n")

                last_id_log = id_log  # atualiza o último ID lido

            time.sleep(intervalo_segundos)

    except KeyboardInterrupt:
        print("⏹️ Monitoramento interrompido manualmente.")
    finally:
        cur_local.close()
        conn_local.close()

if __name__ == "__main__":
    caminho_txt = r"C:\CONEXAO\Connection.txt"

    if not os.path.exists(caminho_txt):
        print(f"❌ Arquivo de configuração não encontrado: {caminho_txt}")
        exit(1)

    config = ler_connection_txt(caminho_txt)

    monitorar_logs(
        dsn_local=config['DSN_LOCAL'],
        user_local=config['USER_LOCAL'],
        pass_local=config['PASS_LOCAL'],
        api_url="http://localhost:8000/replicar",  # coloque a URL da sua API aqui
        intervalo_segundos=1
    )
