import tkinter as tk
from ttkbootstrap import Style
from tkinter.scrolledtext import ScrolledText
from ttkbootstrap.widgets import Button, Label, LabelFrame
from datetime import datetime
import sqlite3
import mysql.connector
import threading
import time
import re


# ==== Conexão com o MySQL ====
def conectar_mysql():
    return mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="20162117",
        database="notas"
    )


# ==== Correção de Dados ====
def corrigir_valor(valor):
    if isinstance(valor, str):
        valor = valor.replace(",", ".")
        valor = re.sub(r"[^\d.]", "", valor)
    try:
        return round(float(valor), 2)
    except:
        return 0.00


def corrigir_linha(linha):
    id_local, data, cliente, cnpj, produto, quantidade, preco_unitario, icms, ipi, pis, cofins, valor_total = linha

    quantidade = corrigir_valor(quantidade)
    preco_unitario = corrigir_valor(preco_unitario)
    icms = corrigir_valor(icms)
    ipi = corrigir_valor(ipi)
    pis = corrigir_valor(pis)
    cofins = corrigir_valor(cofins)
    valor_total = corrigir_valor(valor_total)

    try:
        data = datetime.strptime(data.strip(), '%Y-%m-%d %H:%M:%S')
    except:
        try:
            data = datetime.strptime(data.strip(), '%d/%m/%Y %H:%M')
        except:
            data = datetime.now()

    data = data.strftime('%Y-%m-%d %H:%M:%S')

    return (id_local, data, cliente.strip(), cnpj.strip(), produto.strip(),
            quantidade, preco_unitario, icms, ipi, pis, cofins, valor_total)


# ==== Validação da Linha ====
def validar_linha(linha):
    try:
        datetime.strptime(linha[1], '%Y-%m-%d %H:%M:%S')
        float(linha[5])
        float(linha[6])
        float(linha[7])
        float(linha[8])
        float(linha[9])
        float(linha[10])
        float(linha[11])
        return True
    except:
        return False


# ==== Verificação de Duplicados ====
def dado_ja_existe(cursor_mysql, linha):
    sql = """
        SELECT COUNT(*) FROM notas
        WHERE data = %s AND cliente = %s AND cnpj = %s AND produto = %s AND valor_total = %s
    """
    valores = (linha[1], linha[2], linha[3], linha[4], linha[11])
    cursor_mysql.execute(sql, valores)
    resultado = cursor_mysql.fetchone()
    return resultado[0] > 0


# ==== Tratamento e Envio ====
def tratar_e_enviar_dados():
    try:
        conn_sqlite = sqlite3.connect('C:/Users/Ander/OneDrive/Desktop/VIDA ACADEMICA/Engenharia de Software/PYTHON/projetos para o GitHub/bot_lançador_de_dados/notas.db')
        cursor_sqlite = conn_sqlite.cursor()

        conn_mysql = conectar_mysql()
        cursor_mysql = conn_mysql.cursor()

        cursor_sqlite.execute("SELECT * FROM notas")
        linhas = cursor_sqlite.fetchall()

        total = len(linhas)
        enviados = 0
        ignorados = 0
        rejeitados = 0

        escrever_log(f"🔍 Analisando {total} linhas...")

        for linha in linhas:
            linha_corrigida = corrigir_linha(linha)
            id_local = linha_corrigida[0]
            linha_dados = linha_corrigida[1:]

            if validar_linha(linha_corrigida):
                if not dado_ja_existe(cursor_mysql, linha_corrigida):
                    sql = """
                        INSERT INTO notas (
                            data, cliente, cnpj, produto, quantidade, preco_unitario,
                            icms, ipi, pis, cofins, valor_total
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor_mysql.execute(sql, linha_dados)
                    conn_mysql.commit()
                    enviados += 1
                    escrever_log(f"✔️ Enviado -> ID {id_local} | Cliente: {linha_corrigida[2]} | Produto: {linha_corrigida[4]}")
                else:
                    ignorados += 1
                    escrever_log(f"⚠️ Ignorado (duplicado) -> ID {id_local} | Cliente: {linha_corrigida[2]} | Produto: {linha_corrigida[4]}")
            else:
                rejeitados += 1
                escrever_log(f"❌ Rejeitado (erro nos dados) -> ID {id_local} | Dados inválidos.")

        conn_sqlite.close()
        conn_mysql.close()

        status_label.config(
            text=f"✔️ {enviados} enviados | ⚠️ {ignorados} ignorados | ❌ {rejeitados} rejeitados | Total {total}",
            foreground="green"
        )
    except Exception as e:
        escrever_log(f"❌ Erro durante o processo: {e}")
        status_label.config(text="❌ Erro no processo", foreground="red")


# ==== Reset do MySQL ====
def resetar_mysql():
    try:
        conn_mysql = conectar_mysql()
        cursor_mysql = conn_mysql.cursor()

        cursor_mysql.execute("DELETE FROM notas")
        cursor_mysql.execute("ALTER TABLE notas AUTO_INCREMENT = 1")
        conn_mysql.commit()

        conn_mysql.close()

        escrever_log("🗑️ Banco de dados MySQL resetado com sucesso!")
        status_label.config(text="🗑️ Banco MySQL foi resetado!", foreground="orange")
    except Exception as e:
        escrever_log(f"❌ Erro ao resetar MySQL: {e}")
        status_label.config(text="❌ Erro ao resetar", foreground="red")


# ==== Loop Contínuo com Tempo Personalizado ====
monitorando = False


def loop_continuo():
    global monitorando
    monitorando = True
    atualizar_status_monitoramento()

    try:
        intervalo = int(tempo_entry.get())
        escrever_log(f"⏱️ Monitoramento iniciado. Verificando a cada {intervalo} segundos.")
    except ValueError:
        escrever_log("⚠️ Intervalo inválido. Usando padrão de 60 segundos.")
        intervalo = 60

    while monitorando:
        escrever_log("🚀 Iniciando verificação de dados...")
        tratar_e_enviar_dados()
        escrever_log(f"⏳ Aguardando {intervalo} segundos para próxima verificação...")
        time.sleep(intervalo)

    escrever_log("🛑 Monitoramento interrompido.")
    atualizar_status_monitoramento()


def parar_monitoramento():
    global monitorando
    monitorando = False
    atualizar_status_monitoramento()


def atualizar_status_monitoramento():
    if monitorando:
        monitor_label.config(text="🟢 Monitorando", foreground="green")
    else:
        monitor_label.config(text="🔴 Parado", foreground="red")


# ==== GUI ====
root = tk.Tk()
root.title("Bot de Tratamento de Dados")
style = Style("cyborg")
root.geometry("1000x650")


# ==== Caixa de Log ====
log_frame = LabelFrame(root, text="📜 Log de Execução")
log_frame.pack(fill="both", expand=True, padx=10, pady=10)

log_text = ScrolledText(log_frame, height=20, state="disabled")
log_text.pack(fill="both", expand=True, padx=5, pady=5)


def escrever_log(mensagem):
    log_text.config(state="normal")
    log_text.insert("end", f"{datetime.now().strftime('%H:%M:%S')} - {mensagem}\n")
    log_text.yview("end")
    log_text.config(state="disabled")


# ==== Botões ====
botoes_frame = tk.Frame(root)
botoes_frame.pack(pady=5)

Button(
    botoes_frame, text="🔄 Tratar e Enviar Dados (Único)",
    bootstyle="info", command=tratar_e_enviar_dados
).pack(side="left", padx=5)

Button(
    botoes_frame, text="▶️ Iniciar Monitoramento",
    bootstyle="success", command=lambda: threading.Thread(target=loop_continuo, daemon=True).start()
).pack(side="left", padx=5)

Button(
    botoes_frame, text="⏹️ Parar Monitoramento",
    bootstyle="warning", command=parar_monitoramento
).pack(side="left", padx=5)

Button(
    botoes_frame, text="🗑️ Resetar Banco MySQL",
    bootstyle="danger", command=resetar_mysql
).pack(side="left", padx=5)

Button(
    botoes_frame, text="❌ Fechar",
    bootstyle="secondary", command=root.quit
).pack(side="left", padx=5)


# ==== Configuração de Tempo ====
tempo_frame = tk.Frame(root)
tempo_frame.pack(pady=5)

Label(tempo_frame, text="⏱️ Tempo de verificação (segundos):").pack(side="left", padx=5)

tempo_entry = tk.Entry(tempo_frame, width=5)
tempo_entry.pack(side="left")
tempo_entry.insert(0, "60")


# ==== Status ====
status_label = Label(root, text="", anchor="center", font=("Segoe UI", 10, "bold"))
status_label.pack(pady=5)

monitor_label = Label(root, text="🔴 Parado", font=("Segoe UI", 10, "bold"))
monitor_label.pack(pady=5)


# ==== Iniciar Interface ====
root.mainloop()
