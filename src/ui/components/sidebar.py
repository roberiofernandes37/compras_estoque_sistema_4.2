import customtkinter as ctk
from typing import Callable, Optional

# Cores Adaptativas (Light Mode / Dark Mode)
COLOR_TEXT_PRIMARY = ("#1e293b", "#f1f5f9")  # Slate 800 / Slate 100
COLOR_TEXT_SECONDARY = ("#64748b", "#94a3b8") # Slate 500 / Slate 400
COLOR_BTN_HOVER = ("#e2e8f0", "#334155")

class Sidebar(ctk.CTkFrame):
    """
    Barra Lateral Refinada.
    Foco: Identidade Visual e Acesso a Configurações.
    """

    def __init__(self, master, 
                 titulo: str = "AnalyticX",
                 subtitulo: str = "Gestão de Compras",
                 command_gerar: Optional[Callable] = None, # Mantido p/ compatibilidade, mas não usado visualmente
                 command_config: Optional[Callable] = None,
                 **kwargs):
        
        super().__init__(master, width=200, corner_radius=0, fg_color=("white", "#1e293b"), **kwargs)
        
        self.command_config = command_config
        self.titulo_text = titulo
        self.subtitulo_text = subtitulo

        self._criar_widgets()

    def _criar_widgets(self):
        self.grid_rowconfigure(4, weight=1) 

        # --- 1. Cabeçalho (Identidade) ---
        # Frame azul para destacar o topo
        header = ctk.CTkFrame(self, fg_color="#2563eb", corner_radius=0, height=80)
        header.grid(row=0, column=0, sticky="ew")
        header.grid_propagate(False) # Força altura
        
        lbl_titulo = ctk.CTkLabel(
            header, 
            text=self.titulo_text.upper(), 
            font=("Montserrat", 20, "bold"),
            text_color="white"
        )
        lbl_titulo.pack(side="bottom", pady=(0, 5))

        lbl_sub = ctk.CTkLabel(
            self, 
            text=self.subtitulo_text, 
            font=("Arial", 12),
            text_color=COLOR_TEXT_SECONDARY
        )
        lbl_sub.grid(row=1, column=0, pady=(10, 20))

        # --- 2. Área de Conteúdo (Injetada pelo Launcher) ---
        # Aqui o Launcher vai colocar os inputs de parâmetros.
        # Deixamos o espaço livre rows 2 e 3.

        # --- 3. Footer (Ações Globais) ---
        # Botão Salvar Configurações (Agora mais descritivo)
        self.btn_config = ctk.CTkButton(
            self, 
            text="💾  SALVAR PARÂMETROS", 
            command=self.command_config,
            fg_color="transparent",
            border_width=1,
            border_color=("#cbd5e1", "#475569"),
            text_color=COLOR_TEXT_PRIMARY,
            hover_color=COLOR_BTN_HOVER,
            height=35,
            anchor="center"
        )
        self.btn_config.grid(row=5, column=0, padx=20, pady=10, sticky="ew")

        # Controle de Tema
        self.opt_tema = ctk.CTkOptionMenu(
            self, 
            values=["Light", "Dark", "System"],
            command=self.mudar_tema,
            fg_color=("white", "#334155"),
            button_color=("#e2e8f0", "#475569"),
            button_hover_color=("#cbd5e1", "#64748b"),
            text_color=COLOR_TEXT_PRIMARY,
            dropdown_fg_color=("white", "#334155"),
            dropdown_text_color=COLOR_TEXT_PRIMARY
        )
        self.opt_tema.grid(row=6, column=0, padx=20, pady=(0, 20), sticky="ew")
        self.opt_tema.set("System")

    def mudar_tema(self, novo_tema: str):
        ctk.set_appearance_mode(novo_tema)

    def set_estado_gerar(self, estado: str):
        pass # Método dummy para compatibilidade