import customtkinter as ctk
from typing import Callable, Optional, Dict, List
import time

# --- PALETA DE CORES PROFISSIONAL (Light / Dark) ---
COLOR_BG_MAIN = ("#f3f4f6", "#0f172a")      
COLOR_CARD_BG = ("#ffffff", "#1e293b")      

# Textos
COLOR_TEXT_PRIMARY = ("#1e293b", "#f8fafc") 
COLOR_TEXT_SECONDARY = ("#64748b", "#94a3b8") 

# Acentos
COLOR_PRIMARY = "#2563eb" # Royal Blue
COLOR_SUCCESS = "#16a34a" # Green
COLOR_WARNING = "#f59e0b" # Amber
COLOR_DANGER = "#dc2626"  # Red 600

class Dashboard(ctk.CTkFrame):
    
    def __init__(self, master, **kwargs):
        super().__init__(master, corner_radius=0, **kwargs)
        
        # Estados
        self.var_marca = ctk.StringVar(value="TODAS")
        self.var_busca = ctk.StringVar()
        
        # KPIs Gerais
        self.kpi_valor = ctk.StringVar(value="R$ 0,00")
        self.kpi_skus = ctk.StringVar(value="0")
        self.kpi_pecas = ctk.StringVar(value="0")
        self.kpi_estoque = ctk.StringVar(value="R$ 0,00")
        self.kpi_cobertura = ctk.StringVar(value="0.0 m")
        
        # KPIs de Risco (Completo)
        self.kpi_obs_valor = ctk.StringVar(value="R$ 0,00")
        self.kpi_obs_pct_valor = ctk.StringVar(value="0.0%")
        self.kpi_obs_skus = ctk.StringVar(value="0")
        self.kpi_obs_pct_skus = ctk.StringVar(value="0.0%")
        self.kpi_obs_pecas = ctk.StringVar(value="0")

        # Callbacks
        self.on_simular: Optional[Callable] = None
        self.on_excel: Optional[Callable] = None
        self.on_filter_change: Optional[Callable] = None

        self._construir_layout()

    def _construir_layout(self):
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

        # 1. Sistema de Abas
        self.tabs = ctk.CTkTabview(
            self, 
            fg_color="transparent", 
            text_color=COLOR_TEXT_PRIMARY, 
            segmented_button_fg_color=("white", "#1e293b"),
            segmented_button_selected_color=COLOR_PRIMARY,
            segmented_button_unselected_color=("white", "#1e293b"),
            segmented_button_selected_hover_color="#1d4ed8"
        )
        self.tabs.grid(row=0, column=0, sticky="nsew", padx=20, pady=10)
        
        self.tab_dash = self.tabs.add("  📊 PAINEL DE DECISÃO  ")
        self.tab_log = self.tabs.add("  📝 LOG & AUDITORIA  ")

        self._montar_aba_dashboard()
        self._montar_aba_log()

    def _montar_aba_dashboard(self):
        # --- 1. TOPO (Filtros e Marca) ---
        frame_top = ctk.CTkFrame(self.tab_dash, fg_color=COLOR_CARD_BG, corner_radius=8)
        frame_top.pack(fill="x", pady=(10, 15))
        
        head = ctk.CTkFrame(frame_top, fg_color="transparent")
        head.pack(fill="x", padx=20, pady=15)
        
        ctk.CTkLabel(head, text="MARCA SELECIONADA:", font=("Arial", 11, "bold"), text_color=COLOR_TEXT_SECONDARY).pack(side="left")
        ctk.CTkLabel(head, textvariable=self.var_marca, font=("Arial", 22, "bold"), text_color=COLOR_PRIMARY).pack(side="left", padx=15)
        
        self.entry_busca = ctk.CTkEntry(
            head, placeholder_text="🔍 Filtrar marca...", width=250, height=35, 
            font=("Arial", 12), textvariable=self.var_busca,
            fg_color=("#f1f5f9", "#0f172a"), border_color=("#cbd5e1", "#334155"), text_color=COLOR_TEXT_PRIMARY
        )
        self.entry_busca.pack(side="right")
        self.entry_busca.bind("<KeyRelease>", lambda e: self.on_filter_change(self.var_busca.get()) if self.on_filter_change else None)

        self.scroll_marcas = ctk.CTkScrollableFrame(frame_top, height=80, orientation="vertical", fg_color="transparent")
        self.scroll_marcas.pack(fill="x", padx=20, pady=(0, 20))
        self.lbl_loading = ctk.CTkLabel(self.scroll_marcas, text="Carregando banco de dados...", text_color="gray")
        self.lbl_loading.pack(pady=30)

        # --- 2. AÇÕES (Workflow) ---
        frame_actions = ctk.CTkFrame(self.tab_dash, fg_color="transparent")
        frame_actions.pack(fill="x", pady=5)
        
        self.btn_simular = ctk.CTkButton(
            frame_actions, text="1. PROCESSAR SIMULAÇÃO", height=50, 
            fg_color=COLOR_PRIMARY, font=("Arial", 14, "bold"), corner_radius=8,
            command=lambda: self.on_simular() if self.on_simular else None
        )
        self.btn_simular.pack(side="left", fill="x", expand=True, padx=(0, 10))

        self.btn_excel = ctk.CTkButton(
            frame_actions, text="2. EXPORTAR RELATÓRIO", height=50, 
            fg_color=COLOR_SUCCESS, font=("Arial", 14, "bold"), state="disabled", corner_radius=8,
            command=lambda: self.on_excel() if self.on_excel else None
        )
        self.btn_excel.pack(side="left", fill="x", expand=True, padx=(10, 0))

        # --- 3. KPIS (Resumo Geral) ---
        ctk.CTkLabel(
            self.tab_dash, text="RESUMO GERAL", font=("Arial", 16, "bold"), 
            text_color=COLOR_TEXT_PRIMARY
        ).pack(pady=(25, 10), anchor="center")
        
        grid_kpi = ctk.CTkFrame(self.tab_dash, fg_color="transparent")
        grid_kpi.pack(fill="x")
        grid_kpi.grid_columnconfigure((0,1,2,3,4), weight=1)

        self._criar_card_kpi(grid_kpi, 0, "SUGESTÃO (R$)", self.kpi_valor,   ("#e0f2fe", "#172554"), COLOR_PRIMARY)
        self._criar_card_kpi(grid_kpi, 1, "ITENS (SKU)",   self.kpi_skus,    ("#e0f2fe", "#172554"), COLOR_PRIMARY)
        self._criar_card_kpi(grid_kpi, 2, "PEÇAS (QTD)",   self.kpi_pecas,   ("#e0f2fe", "#172554"), COLOR_PRIMARY)
        self._criar_card_kpi(grid_kpi, 3, "ESTOQUE ATUAL", self.kpi_estoque, ("#f3e8ff", "#581c87"), "#9333ea")
        self._criar_card_kpi(grid_kpi, 4, "COBERTURA",     self.kpi_cobertura,("#f3e8ff", "#581c87"), "#9333ea")

        # --- 4. ABC (AGORA AQUI EM CIMA) ---
        self.frame_abc = ctk.CTkFrame(self.tab_dash, fg_color=COLOR_CARD_BG, corner_radius=8)
        self.frame_abc.pack(fill="both", expand=True, pady=(20, 10))
        
        header_abc = ctk.CTkFrame(self.frame_abc, fg_color="transparent")
        header_abc.pack(fill="x", padx=20, pady=(15, 10))
        
        ctk.CTkLabel(
            header_abc, text="ANÁLISE ESTRATÉGICA ABC (ESTOQUE vs. COMPRA)", 
            font=("Arial", 16, "bold"), text_color=COLOR_TEXT_PRIMARY
        ).pack(anchor="center")

        self.container_abc = ctk.CTkFrame(self.frame_abc, fg_color="transparent")
        self.container_abc.pack(fill="both", expand=True, padx=20, pady=(0, 20))
        self.lbl_abc_placeholder = ctk.CTkLabel(self.container_abc, text="Execute uma simulação para ver a análise ABC.", text_color="gray")
        self.lbl_abc_placeholder.pack(expand=True)

        # --- 5. ALERTA DE RISCO (AGORA AQUI EM BAIXO) ---
        self.frame_risk = ctk.CTkFrame(self.tab_dash, fg_color=("#fee2e2", "#450a0a"), corner_radius=8, border_width=1, border_color=COLOR_DANGER)
        self.frame_risk.pack(fill="x", pady=(10, 25))
        
        # Container Flexível (Horizontal)
        risk_container = ctk.CTkFrame(self.frame_risk, fg_color="transparent")
        risk_container.pack(fill="x", padx=20, pady=8)

        # Coluna 1: Título e Descrição (Esquerda Fixa)
        f_tit = ctk.CTkFrame(risk_container, fg_color="transparent")
        f_tit.pack(side="left", fill="y", padx=(0, 20))
        
        ctk.CTkLabel(f_tit, text="⚠️ ESTOQUE OBSOLETO", font=("Arial", 16, "bold"), text_color=COLOR_DANGER).pack(anchor="w")
        ctk.CTkLabel(f_tit, text="Itens >180d, Saldo >0, Sem Venda >1 ano", font=("Arial", 11), text_color=("#7f1d1d", "#fca5a5")).pack(anchor="w")

        # Separador Vertical
        ctk.CTkFrame(risk_container, width=2, fg_color=("#fecaca", "#7f1d1d")).pack(side="left", fill="y", padx=15)

        # Coluna 2: Valor Financeiro (Expandível)
        f_fin = ctk.CTkFrame(risk_container, fg_color="transparent")
        f_fin.pack(side="left", expand=True, fill="x")
        
        ctk.CTkLabel(f_fin, text="VALOR TRAVADO", font=("Arial", 11, "bold"), text_color=COLOR_TEXT_SECONDARY).pack()
        # Fonte Gigante
        ctk.CTkLabel(f_fin, textvariable=self.kpi_obs_valor, font=("Arial", 22, "bold"), text_color=COLOR_DANGER).pack(pady=(0,2))
        ctk.CTkLabel(f_fin, textvariable=self.kpi_obs_pct_valor, font=("Arial", 13, "bold"), text_color=COLOR_DANGER).pack()

        # Separador Vertical
        ctk.CTkFrame(risk_container, width=2, fg_color=("#fecaca", "#7f1d1d")).pack(side="left", fill="y", padx=15)

        # Coluna 3: Dados Logísticos (Expandível)
        f_log = ctk.CTkFrame(risk_container, fg_color="transparent")
        f_log.pack(side="left", expand=True, fill="x")
        
        # Sub-linha para SKU e Peças
        f_sub = ctk.CTkFrame(f_log, fg_color="transparent")
        f_sub.pack(pady=2)
        
        # SKU
        f_sku = ctk.CTkFrame(f_sub, fg_color="transparent")
        f_sku.pack(side="left", padx=20)
        ctk.CTkLabel(f_sku, text="SKUS MORTOS", font=("Arial", 11, "bold"), text_color=COLOR_TEXT_SECONDARY).pack()
        ctk.CTkLabel(f_sku, textvariable=self.kpi_obs_skus, font=("Arial", 16, "bold"), text_color=COLOR_TEXT_PRIMARY).pack()
        ctk.CTkLabel(f_sku, textvariable=self.kpi_obs_pct_skus, font=("Arial", 11), text_color=COLOR_TEXT_SECONDARY).pack()
        
        # Peças
        f_pec = ctk.CTkFrame(f_sub, fg_color="transparent")
        f_pec.pack(side="left", padx=20)
        ctk.CTkLabel(f_pec, text="TOTAL PEÇAS", font=("Arial", 11, "bold"), text_color=COLOR_TEXT_SECONDARY).pack()
        ctk.CTkLabel(f_pec, textvariable=self.kpi_obs_pecas, font=("Arial", 16, "bold"), text_color=COLOR_TEXT_PRIMARY).pack()

    def _montar_aba_log(self):
        self.txt_log = ctk.CTkTextbox(self.tab_log, font=("Consolas", 13), fg_color=("#ffffff", "#0f172a"), text_color=("#16a34a", "#22c55e"), border_width=1, border_color=("#e2e8f0", "#334155"))
        self.txt_log.pack(fill="both", expand=True, padx=0, pady=0)
        self.txt_log.configure(state="disabled")

    def _criar_card_kpi(self, parent, col, titulo, variavel, bg_tuple, text_color):
        card = ctk.CTkFrame(parent, fg_color=bg_tuple, corner_radius=8, border_width=1, border_color=bg_tuple)
        card.grid(row=0, column=col, padx=5, sticky="ew")
        ctk.CTkLabel(card, text=titulo, font=("Arial", 10, "bold"), text_color=text_color).pack(pady=(15, 0))
        ctk.CTkLabel(card, textvariable=variavel, font=("Arial", 18, "bold"), text_color=COLOR_TEXT_PRIMARY).pack(pady=(2, 15))

    # --- API PÚBLICA ---

    def configurar_acoes(self, on_simular, on_excel, on_filter_change):
        self.on_simular = on_simular
        self.on_excel = on_excel
        self.on_filter_change = on_filter_change

    def renderizar_lista_marcas(self, lista_marcas: List[str]):
        for w in self.scroll_marcas.winfo_children(): w.destroy()
        self.scroll_marcas.grid_columnconfigure((0,1,2,3,4), weight=1)
        sel = self.var_marca.get()
        for i, marca in enumerate(lista_marcas):
            is_sel = (marca == sel)
            btn = ctk.CTkButton(
                self.scroll_marcas, text=marca, height=28, 
                fg_color=COLOR_PRIMARY if is_sel else "transparent",
                text_color="white" if is_sel else COLOR_TEXT_PRIMARY,
                hover_color="#1d4ed8" if is_sel else ("#e2e8f0", "#334155"),
                border_width=1, border_color=("#cbd5e1", "#475569"),
                command=lambda m=marca: self._selecionar_marca_interna(m)
            )
            btn.grid(row=i//5, column=i%5, padx=3, pady=3, sticky="ew")

    def _selecionar_marca_interna(self, marca):
        self.var_marca.set(marca)
        if self.on_filter_change: self.on_filter_change(self.var_busca.get())

    def atualizar_abc_stats(self, abc_data: Dict):
        for w in self.container_abc.winfo_children(): w.destroy()
        
        if not abc_data:
            ctk.CTkLabel(self.container_abc, text="Sem dados ABC disponíveis.", text_color="gray").pack(pady=20)
            return

        total_est = sum(d['estoque'] for d in abc_data.values())
        total_cmp = sum(d['compra'] for d in abc_data.values())

        grid = self.container_abc
        grid.grid_columnconfigure(1, weight=1) 
        grid.grid_columnconfigure(3, weight=1)
        
        ctk.CTkLabel(grid, text="CLASSE", font=("Arial", 11, "bold"), width=60, anchor="w", text_color=COLOR_TEXT_SECONDARY).grid(row=0, column=0, padx=10)
        ctk.CTkLabel(grid, text="ESTOQUE ATUAL", font=("Arial", 11, "bold"), text_color="#9333ea").grid(row=0, column=1, sticky="w")
        ctk.CTkLabel(grid, text="SUGESTÃO COMPRA", font=("Arial", 11, "bold"), text_color=COLOR_PRIMARY).grid(row=0, column=3, sticky="w")

        row_idx = 1
        max_val = max(1, max(v['estoque'] for v in abc_data.values()), max(v['compra'] for v in abc_data.values()))

        def fmt(v, total): 
            pct = (v / total * 100) if total > 0 else 0
            val_str = f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            return f"{val_str} ({pct:.1f}%)"

        for classe in ['A', 'B', 'C']:
            dados = abc_data.get(classe, {'estoque': 0, 'compra': 0})
            
            ctk.CTkLabel(grid, text=f"CURVA {classe}", font=("Arial", 16, "bold"), text_color=COLOR_TEXT_PRIMARY, width=60, anchor="w").grid(row=row_idx, column=0, pady=10, padx=10)

            # Barra Estoque (Mais fina)
            frm_est = ctk.CTkFrame(grid, height=25, fg_color="transparent")
            frm_est.grid(row=row_idx, column=1, sticky="ew", padx=10)
            width_est = (dados['estoque'] / max_val) if max_val > 0 else 0
            if width_est > 0:
                ctk.CTkProgressBar(frm_est, progress_color="#9333ea", height=10).pack(side="left", fill="x", expand=True)
                frm_est.winfo_children()[0].set(width_est)
            # Fonte Maior
            ctk.CTkLabel(frm_est, text=fmt(dados['estoque'], total_est), font=("Arial", 14, "bold"), text_color=COLOR_TEXT_PRIMARY).pack(side="left", padx=8)

            # Barra Compra (Mais fina)
            frm_cmp = ctk.CTkFrame(grid, height=25, fg_color="transparent")
            frm_cmp.grid(row=row_idx, column=3, sticky="ew", padx=10)
            width_cmp = (dados['compra'] / max_val) if max_val > 0 else 0
            if width_cmp > 0:
                ctk.CTkProgressBar(frm_cmp, progress_color=COLOR_PRIMARY, height=10).pack(side="left", fill="x", expand=True)
                frm_cmp.winfo_children()[0].set(width_cmp)
            # Fonte Maior
            ctk.CTkLabel(frm_cmp, text=fmt(dados['compra'], total_cmp), font=("Arial", 14, "bold"), text_color=COLOR_TEXT_PRIMARY).pack(side="left", padx=8)

            row_idx += 2 
            if classe != 'C': ctk.CTkFrame(grid, height=1, fg_color=("#e2e8f0", "#334155")).grid(row=row_idx, column=0, columnspan=5, sticky="ew", pady=5); row_idx += 1

    def get_marca_selecionada(self) -> str: return self.var_marca.get()
    
    def set_estado_processamento(self, processando: bool):
        if processando:
            self.btn_simular.configure(state="disabled", text="⏳ CALCULANDO...", fg_color="gray")
            self.btn_excel.configure(state="disabled")
        else:
            self.btn_simular.configure(state="normal", text="1. REPROCESSAR SIMULAÇÃO", fg_color=COLOR_PRIMARY)
            
    def habilitar_excel(self): self.btn_excel.configure(state="normal", fg_color=COLOR_SUCCESS)

    def log(self, msg: str):
        self.txt_log.configure(state="normal")
        timestamp = time.strftime("[%H:%M:%S] ")
        self.txt_log.insert("end", f"{timestamp}{msg}\n")
        self.txt_log.see("end")
        self.txt_log.configure(state="disabled")
        
    def limpar_log(self):
        self.txt_log.configure(state="normal")
        self.txt_log.delete("1.0", "end")
        self.txt_log.configure(state="disabled")

    def focar_aba_log(self): self.tabs.set("  📝 LOG & AUDITORIA  ")
    def focar_aba_dashboard(self): self.tabs.set("  📊 PAINEL DE DECISÃO  ")

    def atualizar_kpis_dict(self, data: Dict):
        """Atualiza todos os números da tela, incluindo Risco Completo."""
        def fmt(v, prefix="R$ "): 
            if v is None: v = 0
            return f"{prefix}{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            
        def fmt_int(v): 
            if v is None: v = 0
            return f"{int(v):,}".replace(",", ".")

        # KPIs Principais
        self.kpi_valor.set(fmt(data.get('total_valor', 0)))
        self.kpi_skus.set(fmt_int(data.get('total_skus', 0)))
        self.kpi_pecas.set(fmt_int(data.get('total_pecas', 0)))
        self.kpi_estoque.set(fmt(data.get('estoque_atual', 0)))
        self.kpi_cobertura.set(f"{float(data.get('cobertura_meses', 0) or 0):.1f} meses")
        
        # Gráfico ABC
        self.atualizar_abc_stats(data.get('abc_breakdown', {}))
        
        # KPIS DE RISCO (5 Indicadores)
        self.kpi_obs_valor.set(fmt(data.get('obs_valor', 0)))
        self.kpi_obs_pct_valor.set(f"{float(data.get('obs_pct_valor', 0) or 0)*100:.1f}% do valor total")
        
        self.kpi_obs_skus.set(fmt_int(data.get('obs_skus', 0)))
        self.kpi_obs_pct_skus.set(f"{float(data.get('obs_pct_skus', 0) or 0)*100:.1f}% do mix")
        
        self.kpi_obs_pecas.set(fmt_int(data.get('obs_pecas', 0)))

            # MELHOR ATÉ AGORA 18 12 2025 23H15M