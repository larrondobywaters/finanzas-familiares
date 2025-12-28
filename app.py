import streamlit as st
import pandas as pd
import pdfplumber
import re
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Finanzas Familiares", page_icon="💰", layout="wide")

# ==========================================
# 1. CONEXIÓN A GOOGLE SHEETS
# ==========================================
@st.cache_resource
def get_gsheet_client():
    """Conecta a Google usando los secretos de Streamlit Cloud"""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return gspread.authorize(credentials)
    except Exception as e:
        st.error(f"❌ Error de credenciales: {e}")
        return None

def save_to_gsheet(df_new, sheet_name):
    """Guarda datos nuevos evitando duplicados exactos"""
    client = get_gsheet_client()
    if not client: return 0
    
    try:
        sh = client.open(sheet_name)
        worksheet = sh.sheet1
        
        # Obtener datos existentes
        existing_data = worksheet.get_all_records()
        df_current = pd.DataFrame(existing_data)
        
        if df_current.empty:
            df_combined = df_new
        else:
            # Estandarizar columnas para concatenar
            df_combined = pd.concat([df_current, df_new], ignore_index=True)
        
        # Eliminar duplicados (Misma fecha, descripción y monto)
        # Convertimos a string temporalmente para comparar bien
        df_combined['Monto'] = df_combined['Monto'].astype(float)
        df_final = df_combined.drop_duplicates(subset=['Fecha', 'Descripción', 'Monto'], keep='last')
        
        # Escribir todo de nuevo (método más seguro para datasets < 5000 filas)
        worksheet.clear()
        worksheet.update([df_final.columns.values.tolist()] + df_final.values.tolist())
        
        return len(df_final) - len(df_current)
    except Exception as e:
        st.error(f"Error escribiendo en Sheets: {e}")
        return 0

# ==========================================
# 2. MOTORES DE EXTRACCIÓN (ETL)
# ==========================================

def parse_monto_chile(monto_str):
    """Convierte '$ 1.250.000' a float 1250000.0"""
    try:
        clean = monto_str.replace('$', '').replace(' ', '')
        # Formato CL: Puntos son miles, Coma es decimal (a veces)
        # Eliminamos puntos de miles
        clean = clean.replace('.', '')
        # Reemplazamos coma decimal por punto (si hubiese centavos)
        clean = clean.replace(',', '.')
        return float(clean)
    except:
        return 0.0

def extract_cmr_falabella(lines):
    """
    Estrategia específica para Estado de Cuenta CMR.
    Filtra: Avances, Cuotas futuras y Resúmenes basura.
    """
    transactions = []
    total_detected = 0.0
    
    # Regex 1: Busca transacciones reales (Fecha DD/MM/YY + Desc + Monto)
    # Ej: 12/12/23 COMPRA SUPERMERCADO $ 20.000
    rx_tx = re.compile(r'(\d{2}/\d{2}/\d{2,4})\s+(.+?)\s+(-?\$?\s?[\d\.,]+)')
    
    # Regex 2: Busca el "TOTAL A PAGAR" en el encabezado para validar
    rx_total = re.compile(r'(TOTAL A PAGAR|MONTO TOTAL).*?(\$?\s?[\d\.,]+)')

    for line in lines:
        line_upper = line.upper()
        
        # A. Captura del Total del documento (para validación)
        if total_detected == 0:
            match_total = rx_total.search(line_upper)
            if match_total:
                total_detected = parse_monto_chile(match_total.group(2))

        # B. FILTROS DE BASURA (Crucial para tu error anterior)
        # Si la línea tiene "****" y NO dice COMPRA, es basura de la tarjeta
        if "****" in line and "COMPRA" not in line_upper: continue
        # Si es saldo anterior o pagos
        if "SALDO ANTERIOR" in line_upper or "PAGO RECIBIDO" in line_upper: continue
        # Si la descripción es demasiado corta (ej: "**** 0")
        if len(line) < 15: continue
        
        # C. Extracción
        match = rx_tx.search(line)
        if match:
            fecha = match.group(1)
            desc = match.group(2).strip()
            monto_str = match.group(3)
            
            # Filtro extra: Si la descripción es solo numeritos o asteriscos
            if re.match(r'^[\*\s\d]+$', desc): continue

            monto = parse_monto_chile(monto_str)
            
            # CMR muestra gastos en positivo. Los pasamos a negativo.
            if monto > 0: monto = -1 * monto

            transactions.append({
                "Fecha": fecha,
                "Descripción": desc,
                "Monto": monto,
                "Categoría": "Gasto General",
                "Banco_Origen": "CMR Falabella"
            })
            
    return transactions, total_detected

def extract_banco_generico(lines, banco_name):
    """Para BCI y Santander (Formato Cartola)"""
    transactions = []
    # Regex típica cartola: Fecha DD/MM/YY o DD-MM-YY + Desc + Monto
    rx = re.compile(r'(\d{2}[/-]\d{2}[/-]\d{2,4})\s+(.+?)\s+(-?[\d\.]+)')
    
    for line in lines:
        match = rx.search(line)
        if match:
            try:
                monto = parse_monto_chile(match.group(3))
                desc = match.group(2).strip()
                # Filtrar saldos acumulados si aparecen como línea
                if "SALDO" in desc.upper(): continue
                
                transactions.append({
                    "Fecha": match.group(1),
                    "Descripción": desc,
                    "Monto": monto,
                    "Categoría": "Gasto General",
                    "Banco_Origen": banco_name
                })
            except: continue
    return transactions, 0.0 # BCI/Santander es dificil sacar el total del PDF para validar

def extract_sueldo_samsonite(lines):
    """Busca Liquidación de Sueldo"""
    transactions = []
    found = False
    for line in lines:
        if "LÍQUIDO A PAGO" in line.upper() or "A PAGAR" in line.upper():
            # Buscar el último número de la línea
            numeros = re.findall(r'[\d\.]+', line.replace(',', '.')) # simplificado
            if numeros:
                # El ultimo numero suele ser el monto final
                monto = parse_monto_chile(numeros[-1])
                transactions.append({
                    "Fecha": pd.Timestamp.now().strftime("%d/%m/%Y"), # Fecha hoy
                    "Descripción": "Sueldo Samsonite",
                    "Monto": abs(monto), # Ingreso es positivo
                    "Categoría": "Ingreso Familiar",
                    "Banco_Origen": "Liquidación"
                })
                found = True
                break
    return transactions, 0.0

def process_pdf(file):
    with pdfplumber.open(file) as pdf:
        full_text = ""
        for page in pdf.pages:
            full_text += page.extract_text() + "\n"
        
        lines = full_text.split('\n')
        lower_text = full_text.lower()
        
        # DETECTOR DE BANCO
        if "falabella" in lower_text or "cmr" in lower_text:
            return extract_cmr_falabella(lines)
        elif "santander" in lower_text:
            return extract_banco_generico(lines, "Banco Santander")
        elif "bci" in lower_text:
            return extract_banco_generico(lines, "Banco BCI")
        elif "samsonite" in lower_text or "liquidacion" in lower_text:
            return extract_sueldo_samsonite(lines)
        else:
            return [], 0.0

# ==========================================
# 3. LÓGICA DE NEGOCIO (Tus Reglas)
# ==========================================
def apply_rules(df):
    if df.empty: return df
    
    def categorize(row):
        desc = str(row['Descripción']).upper()
        banco = str(row['Banco_Origen']).upper()
        
        # Reglas Prioritarias
        if "LIQUIDACIÓN" in banco: return "Ingreso Familiar"
        if "MARCELA CONTRERAS" in desc: return "Ingreso Familiar" # ¿O transferencia a ella? Ajustar según necesidad
        if "MARCELO CONTRERAS" in desc: return "Arriendo"
        if "EDIPRO" in desc or "CAROL URZUA" in desc: return "Gastos Comunes"
        if "TOTUS" in desc or "LIDER" in desc or "JUMBO" in desc: return "Supermercado"
        
        return row['Categoría'] # Mantiene default

    df['Categoría'] = df.apply(categorize, axis=1)
    return df

# ==========================================
# 4. INTERFAZ GRÁFICA (MAIN)
# ==========================================

st.title("🏡 Finanzas Familiares: Gestor Inteligente")
st.markdown("---")

col_upl, col_stat = st.columns([1, 2])

with col_upl:
    st.subheader("1. Subir Documentos")
    uploaded_files = st.file_uploader(
        "Arrastra tus PDFs (Bancos/Sueldo)", 
        type="pdf", 
        accept_multiple_files=True
    )

if uploaded_files:
    all_txs = []
    
    st.subheader("2. Validación de Extracción")
    
    for pdf in uploaded_files:
        try:
            txs, total_pdf = process_pdf(pdf)
            
            if txs:
                df_temp = pd.DataFrame(txs)
                suma_txs = df_temp['Monto'].sum()
                
                # --- WIDGET DE VALIDACIÓN ---
                # Comparamos valor absoluto para evitar líos de signos
                diff = abs(abs(total_pdf) - abs(suma_txs))
                is_valid = diff < 2000 # Tolerancia $2.000 pesos
                
                icon = "✅" if (is_valid or total_pdf == 0) else "⚠️"
                
                with st.expander(f"{icon} {pdf.name} (Total PDF: ${total_pdf:,.0f})"):
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Total Detectado", f"${total_pdf:,.0f}")
                    c2.metric("Suma Extraída", f"${abs(suma_txs):,.0f}")
                    
                    if total_pdf > 0 and not is_valid:
                        c3.error(f"Diferencia: ${diff:,.0f}")
                        st.warning("La suma de gastos no cuadra con el total del estado de cuenta. Revisa si falta algo.")
                    else:
                        c3.success("Cuadratura OK")
                
                all_txs.extend(txs)
            else:
                st.warning(f"⚠️ No se encontraron datos en {pdf.name}")

        except Exception as e:
            st.error(f"Error procesando {pdf.name}: {e}")

    # --- ETAPA FINAL: EDICIÓN Y GUARDADO ---
    if all_txs:
        df_final = pd.DataFrame(all_txs)
        df_final = apply_rules(df_final)
        
        st.divider()
        st.subheader("3. Revisión Final y Guardado")
        
        # Editor Editable
        edited_df = st.data_editor(
            df_final, 
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "Monto": st.column_config.NumberColumn(format="$%d"),
                "Fecha": st.column_config.TextColumn(),
                "Categoría": st.column_config.SelectboxColumn(
                    options=["Gasto General", "Supermercado", "Arriendo", "Gastos Comunes", "Ingreso Familiar", "Servicios", "Transporte"]
                )
            }
        )
        
        # Botón de Guardado
        col_btn, col_graph = st.columns([1, 3])
        
        with col_btn:
            st.write("") # Espacio
            st.write("") 
            if st.button("💾 Guardar en Google Sheets", type="primary"):
                with st.spinner("Sincronizando..."):
                    added = save_to_gsheet(edited_df, "Finanzas_Master_DB")
                    if added >= 0:
                        st.balloons()
                        st.success(f"¡Listo! Se agregaron {added} transacciones nuevas.")
                    else:
                        st.error("Hubo un problema al guardar.")
        
        with col_graph:
            # Mini Dashboard instantáneo
            gastos = edited_df[edited_df['Monto'] < 0].copy()
            if not gastos.empty:
                gastos['Monto_Abs'] = gastos['Monto'].abs()
                fig = px.pie(gastos, values='Monto_Abs', names='Categoría', hole=0.4, title="Previsualización de Gastos")
                fig.update_layout(height=300, margin=dict(t=30, b=0, l=0, r=0))
                st.plotly_chart(fig, use_container_width=True)

else:
    # Mensaje de bienvenida
    st.info("👆 Sube tus PDFs en la barra lateral para comenzar.")
