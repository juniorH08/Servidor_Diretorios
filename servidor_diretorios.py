"""
Projeto: Servidor de Diretórios com IPC via Socket
Categorias cobertas: DIRETÓRIOS, IPC, REDE (3 categorias, 5+ operações)

Ideia: um processo filho varre um diretório e manda o resultado para o
processo pai por um PIPE (IPC). O pai então abre um SOCKET e entrega
esse resultado para um cliente que se conecta via rede (localhost).
"""

import os
import socket
import threading
import time


DIRETORIO_ALVO = "pasta_teste"
HOST = "127.0.0.1"
PORTA = 5050


def criar_diretorio():
    """[DIRETÓRIOS] Operação 1: Criar um diretório."""
    if not os.path.exists(DIRETORIO_ALVO):
        os.mkdir(DIRETORIO_ALVO)
        # cria alguns arquivos de exemplo só para termos o que listar
        for nome in ["arquivo1.txt", "arquivo2.txt", "notas.md"]:
            open(os.path.join(DIRETORIO_ALVO, nome), "w").close()
    print(f"[OK] Diretório pronto: {DIRETORIO_ALVO}")


def listar_diretorio(caminho):
    """[DIRETÓRIOS] Operação 2: Criar e listar o conteúdo de um diretório."""
    conteudo = os.listdir(caminho)
    print(f"[OK] Conteúdo de '{caminho}': {conteudo}")
    return conteudo


def processo_filho_envia_via_pipe(pipe_escrita):
    """[IPC] Operação 3 (parte filho): processo filho lista o diretório
    e envia o resultado ao pai através de um pipe."""
    conteudo = listar_diretorio(DIRETORIO_ALVO)
    mensagem = ",".join(conteudo)
    os.write(pipe_escrita, mensagem.encode("utf-8"))
    os.close(pipe_escrita)
    print(f"[FILHO PID {os.getpid()}] Enviou dados pelo pipe e vai encerrar.")
    os._exit(0)


def criar_pipe_e_processo():
    """[IPC] Operação 4: Criar pipe + processo filho para comunicação
    entre processos (o pai recebe os dados enviados pelo filho)."""
    pipe_leitura, pipe_escrita = os.pipe()
    pid_filho = os.fork()

    if pid_filho == 0:
        # Processo FILHO: só escreve no pipe
        os.close(pipe_leitura)
        processo_filho_envia_via_pipe(pipe_escrita)
    else:
        # Processo PAI: lê o que o filho mandou
        os.close(pipe_escrita)
        dados = os.read(pipe_leitura, 1024).decode("utf-8")
        os.close(pipe_leitura)
        os.waitpid(pid_filho, 0)
        print(f"[PAI] Recebeu via IPC do filho {pid_filho}: {dados}")
        return dados


def iniciar_servidor(dados_para_enviar):
    """[REDE] Operação 5: Criar um socket servidor que entrega os dados
    (recebidos por IPC) para quem se conectar."""
    def rodar_servidor():
        servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        servidor.bind((HOST, PORTA))
        servidor.listen(1)
        print(f"[SERVIDOR] Escutando em {HOST}:{PORTA}...")

        conexao, endereco = servidor.accept()
        print(f"[SERVIDOR] Cliente conectado: {endereco}")
        conexao.sendall(dados_para_enviar.encode("utf-8"))
        conexao.close()
        servidor.close()

    thread_servidor = threading.Thread(target=rodar_servidor)
    thread_servidor.start()
    time.sleep(0.5)  # dá tempo do servidor subir antes do cliente conectar
    return thread_servidor


def conectar_cliente():
    """[REDE] Operação 6: Cliente que se conecta ao servidor e recebe os
    dados (comunicação cliente/servidor)."""
    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cliente.connect((HOST, PORTA))
    resposta = cliente.recv(1024).decode("utf-8")
    cliente.close()
    print(f"[CLIENTE] Recebeu do servidor: {resposta}")


def main():
    print("=== Servidor de Diretórios com IPC via Socket ===\n")

    criar_diretorio()
    dados = criar_pipe_e_processo()

    thread_servidor = iniciar_servidor(dados)
    conectar_cliente()
    thread_servidor.join()

    print("\n=== Fim da execução ===")


if __name__ == "__main__":
    main()
