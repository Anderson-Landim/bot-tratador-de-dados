import tkinter as tk
from ttkbootstrap import Style
from tkinter.scrolledtext import ScrolledText
from ttkbootstrap.widgets import Button, Label, LabelFrame
from datetime import datetime
import sqlite3
import mysql.connector
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
    id_local, data, cliente, cnpj, produto, preco_unitario, icms, ipi, pis, cofins, valor_total = linha

    # Corrigir valores numéricos
    preco_unitario = corrigir_valor(preco_unitario)
    icms = corrigir_valor(icms)
    ipi = corrigir_valor(ipi)
    pis = corrigir_valor(pis)
    cofins = corrigir_valor(cofins)
    valor_total = corrigir_valor(valor_total)

    # Corrigir data
    try:
        data = datetime.strptime(data.strip(), '%Y-%m-%d %H:%M:%S')
    except:
        try:
            data = datetime.strptime(data.strip(), '%d/%m/%Y %H:%M')
        except:
            data = datetime.now()  # fallback

    data = data.strftime('%Y-%m-%d %H:%M:%S')

    return (id_local, data, cliente.strip(), cnpj.strip(), produto.strip(),
            preco_unitario, icms, ipi, pis, cofins, valor_total)


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
        return True
    except:
        return False


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
        tratados = 0

        for linha in linhas:
            linha_corrigida = corrigir_linha(linha)
            id_local = linha_corrigida[0]
            linha_dados = linha_corrigida[1:]  # remove id_local

            if validar_linha(linha_corrigida):
                sql = """
                    INSERT INTO notas (
                        data, cliente, cnpj, produto, preco_unitario,
                        icms, ipi, pis, cofins, valor_total
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor_mysql.execute(sql, linha_dados)
                conn_mysql.commit()
                tratados += 1
                escrever_log(f"✅ Linha ID {id_local} enviada com sucesso.")
            else:
                escrever_log(f"❌ Linha ID {id_local} ainda contém erros e foi ignorada.")

        conn_sqlite.close()
        conn_mysql.close()

        status_label.config(
            text=f"✔️ Processo concluído! {tratados} de {total} linhas tratadas e enviadas.",
            foreground="green"
        )
    except Exception as e:
        escrever_log(f"❌ Erro durante o processo: {e}")
        status_label.config(text="❌ Erro no processo", foreground="red")


# ==== GUI ====
root = tk.Tk()
root.title("Bot de Tratamento de Dados")
style = Style("cyborg")
root.geometry("1000x600")


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
botoes_frame.pack(pady=15)

Button(
    botoes_frame, text="🔄 Tratar e Enviar Dados",
    bootstyle="info", command=tratar_e_enviar_dados
).pack(side="left", padx=10)

Button(
    botoes_frame, text="❌ Fechar",
    bootstyle="danger", command=root.quit
).pack(side="left", padx=10)


# ==== Status ====
status_label = Label(root, text="", anchor="center", font=("Segoe UI", 10, "bold"))
status_label.pack(pady=10)


root.mainloop()
