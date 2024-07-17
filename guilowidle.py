"""GUI para do APP Low Idle """

import webbrowser
from tkinter import filedialog
from tkinter.messagebox import showerror, showinfo
import customtkinter as ctk
import lowidle


MAIN_WINDOW_TITLE = f"Study Tools - Low idle inspect {lowidle.SCRIPT_VERSION}"
MIN_SIZE_WINDOW_WIDTH = 300
MIN_SIZE_WINDOW_HEIGHT = 150


class ButtomFuntions:
    """Classe para funções dos botões"""

    def __init__(self) -> None:
        self.path_db = ""

    def getbd(self):
        """Pega o caminho do banco de dados"""
        dirname = filedialog.askdirectory(title="Selecione a pasta do BD do Cliente")
        if dirname:
            print("Pasta de Destino: " + str(dirname) + "\n")
            self.path_db = dirname

    def run_powerpf(self) -> None:
        """Executa a rotina do PowerProfile"""
        if not self.path_db:
            tx_error = "Erro: Caminho do BD Cliente inválido!"
            print(tx_error)
            showerror("Erro", tx_error)

        lowidle.main(self.path_db)
        showinfo("Sucesso!", "Resultados obitidos com sucesso!")


def put_gadgets_main(app: ctk.CTk) -> None:
    """Coloca gadgets da janela principal"""

    runbt = ButtomFuntions()

    tx_title = "Study Tools - Low idle inspect"
    lb_title = ctk.CTkLabel(app, text=tx_title)
    lb_title.pack(side="top")

    bt_db = ctk.CTkButton(master=app, text="BD Cliente", command=runbt.getbd)
    bt_run = ctk.CTkButton(
        master=app, text="Executar", fg_color="Red", command=runbt.run_powerpf
    )

    bt_db.place(relx=0.50, rely=0.30, anchor=ctk.CENTER)
    bt_run.place(relx=0.50, rely=0.6, anchor=ctk.CENTER)

    text_about = lowidle.SCRIPT_VERSION + " - By Pedro Venancio - Sobre / Ajuda"
    lb_about = ctk.CTkLabel(app, text=text_about, text_color="blue")
    lb_about.pack(side="bottom")
    lb_about.bind(
        "<Button-1>",
        lambda x: webbrowser.open_new("https://www.linkedin.com/in/pedrobvenancio/"),
    )


def main() -> None:
    """Cria a janela principal"""

    ctk.set_appearance_mode("dark")
    ctk.set_default_color_theme("blue")

    app = ctk.CTk()
    app.minsize(width=MIN_SIZE_WINDOW_WIDTH, height=MIN_SIZE_WINDOW_HEIGHT)
    app.title(MAIN_WINDOW_TITLE)
    app.geometry(f"{MIN_SIZE_WINDOW_WIDTH} X {MIN_SIZE_WINDOW_HEIGHT}")
    app.resizable(False, False)

    put_gadgets_main(app)

    app.mainloop()


if __name__ == "__main__":

    print(
        "Bem-vindo ao PowerProfile! \n",
        "Versão: ",
        lowidle.SCRIPT_VERSION,
        "\n",
    )

    main()
