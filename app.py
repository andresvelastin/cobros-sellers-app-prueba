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
st.set_page_config(page_title="Test de conexión Redshift", layout="centered")
st.title("🔍 Prueba de conexión a Redshift")
st.markdown("""
Esta aplicación permite probar la conexión con **Amazon Redshift (Datalake)**  
e inspeccionar si el SKU consultado devuelve resultados.
""")

# Campo para ingresar el SKU
sku_input = st.text_input("Ingrese un SKU Marketplace:", placeholder="Ejemplo: MKLC0NLUP0-4")

# Botón para ejecutar la consulta
if st.button("Ejecutar consulta"):
    st.write("➡️ **Botón presionado, comenzando ejecución...**")

    if sku_input.strip() == "":
        st.warning("⚠️ Por favor, ingrese un SKU antes de ejecutar la consulta.")
    else:
        try:
            st.write("🔄 Intentando conectar a Redshift...")
            conn = psycopg2.connect(**DB_CONFIG, sslmode="require")
            st.success("✅ Conexión establecida correctamente con Redshift.")

            st.write("📦 Ejecutando query...")

            query = f"""
                SELECT seller, fecha_de_orden, nombre_producto, estado
                FROM cencosud_chile_txd_reporter.ordenes_mkp
                WHERE sku_marketplace IN ('{sku_input}')
                LIMIT 5;
            """

            df_result = pd.read_sql_query(query, conn)
            conn.close()

            st.write("📊 Query ejecutada, mostrando resultados...")

            if df_result.empty:
                st.warning("⚠️ No se encontraron resultados para ese SKU.")
            else:
                st.success(f"✅ Consulta ejecutada correctamente. Mostrando {len(df_result)} filas:")
                st.dataframe(df_result)

        except Exception as e:
            st.error("❌ Error al conectar o ejecutar la consulta:")
            st.code(str(e))





