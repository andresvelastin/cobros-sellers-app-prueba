import streamlit as st
import pandas as pd
import psycopg2

# ------------------------------------
# CONFIGURACIÓN DE CONEXIÓN REDSHIFT
# ------------------------------------
DB_CONFIG = {
    "host": "datalake-redshift-cluster-v2.clfk1dkmkw9l.us-east-1.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "datalake",
    "user": "cl_txd_marketplace_ro",
    "password": "D2brUfveQk587aL&2y!5#q4"
}

# ------------------------------------
# INTERFAZ DE USUARIO
# ------------------------------------
st.set_page_config(page_title="Consulta Redshift - Proyecto Sellers", layout="wide")
st.title("🧾 Consulta a Redshift - Proyecto de Cobros a Sellers")
st.markdown("Sube tu tabla de productos o pégala aquí abajo para consultar información relacionada en Redshift.")

# ------------------------------------
# OPCIÓN 1: Subir archivo Excel/CSV
# ------------------------------------
uploaded_file = st.file_uploader("📤 Sube archivo (CSV o Excel)", type=["csv", "xlsx"])

# ------------------------------------
# OPCIÓN 2: Pegar tabla directamente
# ------------------------------------
pasted_text = st.text_area("📋 O pega tu tabla aquí (desde Excel)", height=200, placeholder="Pega aquí tu tabla...")

# Convertir texto pegado en DataFrame
df_input = None
if pasted_text.strip() != "":
    try:
        df_input = pd.read_csv(pd.io.common.StringIO(pasted_text), sep="\t")  # pegado tipo Excel (tabulado)
    except Exception:
        df_input = pd.read_csv(pd.io.common.StringIO(pasted_text), sep=",")   # fallback CSV

elif uploaded_file:
    if uploaded_file.name.endswith(".csv"):
        df_input = pd.read_csv(uploaded_file)
    else:
        df_input = pd.read_excel(uploaded_file)

# ------------------------------------
# MOSTRAR TABLA PEGADA
# ------------------------------------
if df_input is not None:
    st.success("✅ Tabla cargada correctamente")
    st.dataframe(df_input.head())

    # Extraer SKUs únicos
    skus = df_input["SKU CENCOSUD"].astype(str).unique().tolist()

    # ------------------------------------
    # CONSULTAR EN REDSHIFT
    # ------------------------------------
    if st.button("🔍 Ejecutar consulta en Redshift"):
        try:
            conn = psycopg2.connect(**DB_CONFIG, sslmode="require")

            # Generar lista SQL ('SKU1','SKU2','SKU3',...)
            sku_list = ",".join([f"'{sku}'" for sku in skus])

            query = f"""
                SELECT 
                    s.name AS seller,
                    st.sku,
                    st.quantity AS stock,
                    st.warehouse
                FROM "eiffel-stock".stock st
                JOIN eiffel_seller.seller s ON s.id = st.seller_id
                WHERE st.sku IN ({sku_list})
                AND st.is_fulfillment = 1
                AND st.active = 1
                LIMIT 200;
            """

            df_result = pd.read_sql_query(query, conn)
            conn.close()

            if df_result.empty:
                st.warning("⚠️ No se encontraron coincidencias en Redshift.")
            else:
                st.success("✅ Consulta completada correctamente")
                st.dataframe(df_result)

                # Botón para descargar
                csv = df_result.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="⬇️ Descargar resultados (CSV)",
                    data=csv,
                    file_name="resultados_redshift.csv",
                    mime="text/csv"
                )

        except Exception as e:
            st.error(f"❌ Error al conectar o ejecutar la consulta: {e}")
else:
    st.info("👆 Sube o pega una tabla para comenzar.")
