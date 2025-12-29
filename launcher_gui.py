import customtkinter as ctk
import sys
import threading
import subprocess
import json
import duckdb
from pathlib import Path
import yaml
from tkinter import messagebox
import os

# --- IMPORTAÇÃO DOS COMPONENTES VISUAIS ---
try:
    from src.ui.components.sidebar import Sidebar
    from src.ui.components.dashboard import Dashboard
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from src.ui.components.sidebar import Sidebar
    from src.ui.components.dashboard import Dashboard

# Configuração Global de Tema
ctk.set_appearance_mode("Light")
ctk.set_default_color_theme("blue")

# COR DE FUNDO PRINCIPAL (Ajuste para estilo "Windows Moderno")
# Light: Cinza muito suave (#f3f4f6) | Dark: Azul Profundo (#0f172a)
COLOR_BG_MAIN = ("#f3f4f6", "#0f172a")

class DashboardApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        # Setup Janela
        self.title("NewCompras v7.4 - Enterprise Edition")
        self.geometry("1280x850")
        self.minsize(900, 600)
        
        # Paths
        self.root_dir = Path(__file__).parent
        self.config_path = self.root_dir / "config" / "parametros.yaml"
        self.script_path = self.root_dir / "scripts" / "gerar_relatorio_final.py"
        self.db_path = self.root_dir / "data" / "vendas.db"
        self.cache_path = self.root_dir / "data" / "marcas_cache.json"

        # Variáveis de Configuração
        self.var_cobertura = ctk.StringVar()
        self.var_lead_time = ctk.StringVar()
        self.var_dias_novo = ctk.StringVar()
        
        # Cache de Marcas
        self.todas_marcas = ["TODAS"]

        # --- LAYOUT PRINCIPAL ---
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self._construir_interface()
        self._inicializar_dados()

    def _construir_interface(self):
        # 1. Sidebar (Menu Lateral)
        self.sidebar = Sidebar(
            master=self,
            titulo="AnalyticX",
            subtitulo="Gestão Inteligente v7.4",
            command_gerar=lambda: self.iniciar_processamento(simulacao=True),
            command_config=self.salvar_parametros
        )
        self.sidebar.grid(row=0, column=0, rowspan=4, sticky="nsew")

        # Injeção de Inputs na Sidebar
        self.frame_inputs = ctk.CTkFrame(self.sidebar, fg_color="transparent")
        self.frame_inputs.grid(row=4, column=0, sticky="new", padx=10, pady=(20, 0))
        
        self.criar_grupo_input_custom(self.frame_inputs, "PARÂMETROS GLOBAIS", [
            ("Meta Cobertura (Meses):", self.var_cobertura),
            ("Lead Time Padrão (Dias):", self.var_lead_time),
        ])
        self.criar_grupo_input_custom(self.frame_inputs, "DEFINIÇÃO DE PRODUTO", [
            ("Janela 'Item Novo' (Dias):", self.var_dias_novo),
        ])
        
        # Botão Recarregar com cor adaptativa
        ctk.CTkButton(
            self.frame_inputs, 
            text="🔄 Recarregar Marcas", 
            fg_color=("#e2e8f0", "#334155"), # Cinza claro no Light
            text_color=("#1e293b", "#ffffff"), 
            hover_color=("#cbd5e1", "#475569"),
            height=32,
            command=lambda: threading.Thread(target=self.forcar_atualizacao_marcas, daemon=True).start()
        ).pack(pady=(20,0), fill="x")

        # 2. Dashboard (Área Principal)
        # Passamos a cor de fundo explicitamente aqui
        self.dashboard = Dashboard(self, fg_color=COLOR_BG_MAIN)
        self.dashboard.grid(row=0, column=1, sticky="nsew")
        
        # Conecta callbacks
        self.dashboard.configurar_acoes(
            on_simular=lambda: self.iniciar_processamento(simulacao=True),
            on_excel=lambda: self.iniciar_processamento(simulacao=False),
            on_filter_change=self.filtrar_marcas
        )

    def criar_grupo_input_custom(self, parent, titulo, campos):
        """
        Cria inputs com contraste corrigido para garantir legibilidade no tema Light.
        """
        # Título
        ctk.CTkLabel(
            parent, 
            text=titulo, 
            text_color=("#1e40af", "#94a3b8"), # Azul escuro no Light 
            font=("Arial", 11, "bold"), 
            anchor="w"
        ).pack(fill="x", pady=(15, 5))
        
        for label_text, variable in campos:
            # Rótulo
            ctk.CTkLabel(
                parent, 
                text=label_text, 
                text_color=("#1e293b", "#e2e8f0"), # Preto no Light
                font=("Arial", 12), 
                anchor="w"
            ).pack(fill="x")
            
            # Campo de Texto (Branco no Light para destacar do fundo cinza)
            ctk.CTkEntry(
                parent, 
                textvariable=variable, 
                height=30, 
                fg_color=("#ffffff", "#0f172a"), 
                border_color=("#94a3b8", "#334155"), 
                text_color=("#000000", "#ffffff")
            ).pack(fill="x", pady=(0, 10))

    def _inicializar_dados(self):
        self.carregar_parametros()
        threading.Thread(target=self.gerenciar_cache_marcas, daemon=True).start()

    # --- LÓGICA DE MARCAS (Mantida Integralmente) ---
    def gerenciar_cache_marcas(self):
        if self.cache_path.exists():
            try:
                self.dashboard.log("📂 Carregando marcas do cache...")
                with open(self.cache_path, 'r', encoding='utf-8') as f:
                    self.todas_marcas = json.load(f)
                    self.after(0, lambda: self.filtrar_marcas(""))
                    self.dashboard.log(f"✅ {len(self.todas_marcas)} marcas carregadas.")
                    return
            except Exception: pass
        self.forcar_atualizacao_marcas()

    def forcar_atualizacao_marcas(self):
        self.dashboard.log("⏳ Indexando marcas do banco...")
        try:
            if not self.db_path.exists(): return
            con = duckdb.connect(":memory:")
            con.execute(f"ATTACH '{str(self.db_path)}' AS sqlite_db (TYPE SQLITE, READ_ONLY)")
            res = con.execute("SELECT DISTINCT marca FROM sqlite_db.produtos_gerais WHERE marca IS NOT NULL AND marca != '' ORDER BY 1").fetchall()
            self.todas_marcas = ["TODAS"] + [str(r[0]) for r in res]
            
            with open(self.cache_path, 'w', encoding='utf-8') as f: json.dump(self.todas_marcas, f)
            
            self.after(0, lambda: self.filtrar_marcas(""))
            self.dashboard.log(f"✅ Indexação concluída.")
            con.close()
        except Exception as e: self.dashboard.log(f"❌ Erro marcas: {e}")

    def filtrar_marcas(self, termo=""):
        termo = termo.upper() if termo else ""
        lista = [m for m in self.todas_marcas if termo in m.upper()] if termo else self.todas_marcas
        self.dashboard.renderizar_lista_marcas(lista)

    # --- MOTOR DE CÁLCULO (Mantido Integralmente) ---
    def iniciar_processamento(self, simulacao=True):
        self.salvar_parametros()
        if not simulacao: self.dashboard.focar_aba_log()
        
        self.sidebar.set_estado_gerar("disabled")
        self.dashboard.set_estado_processamento(True)
        self.dashboard.limpar_log()

        threading.Thread(target=lambda: self.rodar_script(simulacao), daemon=True).start()

    def rodar_script(self, simulacao):
        try:
            marca = self.dashboard.get_marca_selecionada()
            python_exec = sys.executable 
            cmd = [python_exec, str(self.script_path), "--marca", marca]
            if simulacao: cmd.append("--simulacao")

            self.dashboard.log(f"🚀 Iniciando motor: {marca}")
            
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1, encoding='utf-8', errors='replace')

            for line in process.stdout:
                line_clean = line.strip()
                if "--- LOG START ---" not in line_clean and line_clean:
                    self.after(0, lambda l=line_clean: self.dashboard.log(l))

            process.wait()
            
            if process.stderr.read(): self.dashboard.log(f"🔴 STDERR detectado.")

            if process.returncode == 0:
                self._carregar_resultados()

            self.after(0, lambda: self.finalizar_processo(simulacao, process.returncode))

        except Exception as e:
            self.dashboard.log(f"❌ ERRO CRÍTICO: {e}")
            self.after(0, lambda: self.finalizar_processo(simulacao, 1))

    def _carregar_resultados(self):
        try:
            stats_path = self.root_dir / "data" / "cache" / "last_run_stats.json"
            if stats_path.exists():
                with open(stats_path, 'r', encoding='utf-8') as f:
                    data = json.load(f).get("data", {})
                    self.after(0, lambda: self.dashboard.atualizar_kpis_dict(data))
        except Exception: pass

    def finalizar_processo(self, simulacao, codigo_retorno):
        self.dashboard.set_estado_processamento(False)
        self.sidebar.set_estado_gerar("normal")
        
        if codigo_retorno == 0:
            if simulacao:
                self.dashboard.habilitar_excel()
                self.dashboard.focar_aba_dashboard()
                self.dashboard.log("✅ Simulação concluída.")
                messagebox.showinfo("Sucesso", "Simulação finalizada!")
            else:
                self.dashboard.log("✅ Excel gerado.")
                messagebox.showinfo("Sucesso", "Relatório gerado!")
                try: os.startfile(str(self.root_dir / "data" / "exports"))
                except: pass
        else:
            self.dashboard.focar_aba_log()
            messagebox.showerror("Erro", "Ocorreu uma falha. Verifique o Log.")

    # --- CONFIGURAÇÃO ---
    def carregar_parametros(self):
        try:
            if not self.config_path.exists(): return
            with open(self.config_path, 'r', encoding='utf-8') as f: data = yaml.safe_load(f)
            self.var_cobertura.set(str(data.get('compras', {}).get('meses_cobertura', 1.5)))
            self.var_lead_time.set(str(data.get('lead_time', {}).get('padrao_dias', 10)))
            self.var_dias_novo.set(str(data.get('produto', {}).get('dias_lancamento', 60)))
        except: pass

    def salvar_parametros(self):
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f: data = yaml.safe_load(f) or {}
            if 'compras' not in data: data['compras'] = {}
            if 'lead_time' not in data: data['lead_time'] = {}
            if 'produto' not in data: data['produto'] = {}

            data['compras']['meses_cobertura'] = float(self.var_cobertura.get())
            data['lead_time']['padrao_dias'] = int(self.var_lead_time.get())
            data['produto']['dias_lancamento'] = int(self.var_dias_novo.get())
            
            with open(self.config_path, 'w', encoding='utf-8') as f: yaml.dump(data, f)
            messagebox.showinfo("Salvo", "Parâmetros atualizados!")
        except Exception as e: messagebox.showerror("Erro", str(e))

if __name__ == "__main__":
    app = DashboardApp()
    app.mainloop()

    # MELHOR ATÉ AGORA 18 12 2025 23H15M