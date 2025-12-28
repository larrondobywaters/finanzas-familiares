import streamlit as st
import pandas as pd
import pdfplumber
import re
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Finanzas Familiares ETL", layout="wide")

# --- 1. CONEXIÓN A GOOGLE SHEETS (SINGLETON) ---
# Usamos caché para no reconectar en cada interacción
@st.cache_resource
def get_gsheet_client():
    # Definimos el alcance (scope) para leer y escribir
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    # Cargamos las credenciales desde st.secrets
    # Streamlit permite acceder a los secretos como un diccionario
    creds_dict = dict(st.secrets["gcp_service_account"])
    
    credentials = Credentials.from_service_account_info(
        creds_dict,
        scopes=scopes
    )
    
    client = gspread.authorize(credentials)
    return client

def load_master_db(sheet_name):
    client = get_gsheet_client()
    try:
        sh = client.open(sheet_name)
        worksheet = sh.sheet1
        # Obtenemos todos los registros como lista de dicts
        data = worksheet.get_all_records()
        if not data:
            return pd.DataFrame(columns=["Fecha", "Descripción", "Monto", "Categoría", "Banco_Origen"])
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error conectando a la Sheet: {e}")
        return pd.DataFrame()

def save_to_gsheet(df_new, sheet_name):
    client = get_gsheet_client()
    sh = client.open(sheet_name)
    worksheet = sh.sheet1
    
    # Leemos datos actuales para chequear duplicados
    df_current = load_master_db(sheet_name)
    
    # Concatenamos
    df_combined = pd.concat([df_current, df_new], ignore_index=True)
    
    # --- LOGICA DE DUPLICADOS ---
    # Eliminamos duplicados exactos basados en Fecha, Descripción y Monto
    # Convertimos a string para asegurar comparación correcta antes de borrar
    df_final = df_combined.drop_duplicates(subset=['Fecha', 'Descripción', 'Monto'], keep='last')
    
    # Escribimos de vuelta (Update total es más seguro para integridad en apps pequeñas)
    worksheet.clear()
    worksheet.update([df_final.columns.values.tolist()] + df_final.values.tolist())
    return len(df_final) - len(df_current)

# --- 2. LOGICA DE PARSING (EL NÚCLEO) ---

def parse_monto(monto_str):
    """Limpia strings de moneda chilenos ej: $1.200.000 -> 1200000.0"""
    try:
        clean = monto_str.replace('$', '').replace('.', '').replace(',', '.')
        return float(clean)
    except:
        return 0.0

def detect_bank_strategy(text):
    text_lower = text.lower()
    if "puntos cmr" in text_lower or "falabella" in text_lower:
        return "CMR Falabella"
    elif "santander" in text_lower:
        return "Banco Santander"
    elif "bci" in text_lower:
        return "Banco BCI"
    elif "samsonite" in text_lower or "liquidacion" in text_lower or "haberes" in text_lower:
        return "Liquidación Sueldo"
    return "Desconocido"

def extract_data_from_pdf(uploaded_file):
    """Función maestra que orquesta la extracción"""
    transactions = []
    
    with pdfplumber.open(uploaded_file) as pdf:
        full_text = ""
        for page in pdf.pages:
            full_text += page.extract_text() + "\n"
            
        banco = detect_bank_strategy(full_text)
        
        # --- PARSING ESPECÍFICO (Simplificado con Regex) ---
        # NOTA: Los regex dependen 100% del formato visual del PDF.
        # Aquí pongo ejemplos genéricos robustos que deberás ajustar viendo tus PDFs.
        
        lines = full_text.split('\n')
        
        for line in lines:
            try:
                if banco == "CMR Falabella":
                    # Ejem: 12/12/2025 COMPRA SUPERMERCADO $50.000
                    match = re.search(r'(\d{2}/\d{2}/\d{4})\s+(.+?)\s+\$([\d\.]+)', line)
                    if match:
                        transactions.append({
                            "Fecha": match.group(1),
                            "Descripción": match.group(2).strip(),
                            "Monto": parse_monto(match.group(3)) * -1, # Gasto negativo
                            "Categoría": "Gasto General",
                            "Banco_Origen": banco
                        })

                elif banco == "Banco Santander" or banco == "Banco BCI":
                    # Cartola típica: 12-12-25 TRASPASO A TERCEROS -10000
                    # Busca patrones de fecha al inicio
                    match = re.search(r'(\d{2}[-/]\d{2}[-/]\d{2,4})\s+(.+?)\s+(-?[\d\.]+)', line)
                    if match:
                        transactions.append({
                            "Fecha": match.group(1),
                            "Descripción": match.group(2).strip(),
                            "Monto": parse_monto(match.group(3)),
                            "Categoría": "Gasto General",
                            "Banco_Origen": banco
                        })
                
                elif banco == "Liquidación Sueldo":
                    # Buscamos el "Líquido a Pago" o similar
                    if "LÍQUIDO A PAGO" in line.upper() or "A PAGAR" in line.upper():
                        # Buscar el último número en la línea
                        numbers = re.findall(r'[\d\.]+', line)
                        if numbers:
                            monto = parse_monto(numbers[-1])
                            transactions.append({
                                "Fecha": pd.Timestamp.now().strftime("%d/%m/%Y"), # Asumimos mes actual
                                "Descripción": "Sueldo Samsonite",
                                "Monto": monto,
                                "Categoría": "Ingreso Familiar",
                                "Banco_Origen": "Liquidación"
                            })
                            break # Solo nos interesa una línea en la liquidación

            except Exception as e:
                continue # Si falla una línea, seguimos con la siguiente

    return pd.DataFrame(transactions)

# --- 3. LÓGICA DE NEGOCIO (REGLAS) ---
def apply_business_rules(df):
    if df.empty: return df
    
    def classify(row):
        desc = str(row['Descripción']).upper()
        banco = str(row['Banco_Origen']).upper()
        
        # Regla 1: Ingresos
        if "LIQUIDACIÓN" in banco or "MARCELA CONTRERAS" in desc:
            return "Ingreso Familiar"
        
        # Regla 2: Arriendo
        if "MARCELO CONTRERAS" in desc: # Ojo: Ajustar si es Marcela o Marcelo según tu prompt
            return "Arriendo"
            
        # Regla 3: GC
        if "EDIPRO" in desc or "CAROL URZUA" in desc:
            return "Gastos Comunes"
            
        return row['Categoría'] # Mantener original si no hay match

    df['Categoría'] = df.apply(classify, axis=1)
    return df

# --- 4. INTERFAZ GRÁFICA (FRONTEND) ---

st.title("💰 Gestor de Finanzas Familiares")
st.markdown("### Arquitectura de Datos Centralizada")

# Sección Lateral para Upload
with st.sidebar:
    st.header("Ingesta de Datos")
    uploaded_files = st.file_uploader("Sube tus PDFs (Bancos/Liquidaciones)", 
                                      type="pdf", accept_multiple_files=True)

if uploaded_files:
    all_data = []
    st.info(f"Procesando {len(uploaded_files)} archivos...")
    
    for pdf_file in uploaded_files:
        try:
            df_pdf = extract_data_from_pdf(pdf_file)
            if not df_pdf.empty:
                all_data.append(df_pdf)
            else:
                st.warning(f"No se extrajeron datos de: {pdf_file.name}")
        except Exception as e:
            st.error(f"Error crítico leyendo {pdf_file.name}: {e}")

    if all_data:
        # Consolidar
        df_full = pd.concat(all_data, ignore_index=True)
        
        # Aplicar reglas de negocio
        df_full = apply_business_rules(df_full)
        
        st.subheader("🔍 Revisión y Edición (Staging Area)")
        st.markdown("Corrige las categorías antes de enviar a la Base de Datos Maestra.")
        
        # Editor interactivo
        edited_df = st.data_editor(df_full, num_rows="dynamic")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("💾 Guardar en Google Sheets", type="primary"):
                with st.spinner("Sincronizando con la nube..."):
                    new_rows = save_to_gsheet(edited_df, "Finanzas_Master_DB")
                    st.success(f"¡Éxito! Se añadieron {new_rows} registros nuevos (se ignoraron duplicados).")
        
        # --- DASHBOARD PRELIMINAR ---
        st.divider()
        st.subheader("📊 Análisis Rápido (Datos subidos)")
        
        # Filtramos solo gastos (montos negativos o positivos según tu lógica, aquí asumo gastos negativos)
        # Ajusta esta lógica si tus gastos vienen positivos en el PDF
        gastos_df = edited_df[edited_df['Categoría'] != 'Ingreso Familiar']
        
        if not gastos_df.empty:
            # Asegurar montos absolutos para el gráfico
            gastos_df['Monto_Abs'] = gastos_df['Monto'].abs()
            
            fig = px.pie(gastos_df, values='Monto_Abs', names='Categoría', 
                         title='Distribución de Gastos (Carga Actual)')
            st.plotly_chart(fig)
            
    else:
        st.warning("No se pudieron detectar transacciones válidas. Revisa el formato de los PDFs.")

else:
    # Mostrar estado actual de la DB si no hay carga
    st.write("---")
    st.subheader("Estado Actual de la Base de Datos")
    if st.button("Cargar datos históricos"):
        existing_df = load_master_db("Finanzas_Master_DB")
        st.dataframe(existing_df)