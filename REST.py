import threading
import time
from flask import Flask, jsonify
import schedule
from sqlalchemy import create_engine
import pandas as pd
from flask_cors import CORS  # Importa la extensión CORS
import ETL

app = Flask(__name__)

# Configura CORS para permitir solicitudes de cualquier origen
CORS(app, resources={r"/*": {"origins": "*"}})

# Conexión a la base de datos PostgreSQL
postgres_connection = create_engine('postgresql://postgres:root@localhost:5432/postgres')

# Simulación de la función ETL
class ETL:
    @staticmethod
    def ejecutarCargaBatch():
        # Aquí colocas tu lógica de carga por lotes
        print("Ejecución del proceso de carga por lotes...")

@app.route('/cargaBatch', methods=['GET'])
def ejecutarCargaBatch():
    try:
        ETL.ejecutarCargaBatch()
        return jsonify({"mensaje": "Proceso de carga por lotes ejecutado exitosamente."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/ventas-mensuales', methods=['GET'])
def ventas_mensuales():
    query = "SELECT * FROM Ventas"
    df = pd.read_sql(query, postgres_connection)
    
    df['fecha'] = pd.to_datetime(df['fecha'])
    df['Year'] = df['fecha'].dt.year
    df['Month'] = df['fecha'].dt.month
    
    df_grouped = df.groupby(['Year', 'Month'], as_index=False)['total'].sum()
    
    df_grouped.rename(columns={'total': 'TotalVentas'}, inplace=True)
    
    result = df_grouped.to_dict(orient='records')

    return jsonify(result)

@app.route('/estado-pedidos', methods=['GET'])
def estado_pedidos():
    try:
        query = """
        SELECT 
            ep."Estado" AS EstadoPedido, 
            COUNT(p.Id) AS TotalPedidos
        FROM 
             public.pedidos p
        JOIN 
             public."EstadosPedido" ep ON p.IdEstado = ep."Id"
        GROUP BY 
            ep."Estado";
        """
        df = pd.read_sql(query, postgres_connection)

        result = df.to_dict(orient='records')

        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": "Error inesperado", "details": str(e)}), 500

@app.route('/clientes-activos', methods=['GET'])
def clientes_activos():
    try:
        pedidos_query = "SELECT * FROM Pedidos"
        ventas_query = "SELECT * FROM Ventas"
        usuarios_query = "SELECT * FROM AspNetUsers"
        
        df_pedidos = pd.read_sql_query(pedidos_query, postgres_connection)
        df_ventas = pd.read_sql_query(ventas_query, postgres_connection)
        df_usuarios = pd.read_sql_query(usuarios_query, postgres_connection)

        df_pedidos['fechapedido'] = pd.to_datetime(df_pedidos['fechapedido'])
        
        df_ventas = df_ventas.rename(columns={'id': 'idventa'})
                
        df_combined = df_pedidos.merge(df_ventas, on='idventa', how='inner')
        
        df_combined = df_combined[df_combined['idusuario'].isin(df_usuarios['id'])]
        

        df_combined['Year'] = df_combined['fechapedido'].dt.year
        df_combined['Month'] = df_combined['fechapedido'].dt.month

        df_result = df_combined.groupby(['Year', 'Month']) \
                                  .agg(ClientesActivos=('idusuario', 'nunique')) \
                                  .reset_index()

        result = df_result.to_dict(orient='records')

        return jsonify(result), 200
    
    except Exception as e:
        return jsonify({"error": "Error inesperado", "details": str(e)}), 500

@app.route('/tasa-crecimiento-ventas', methods=['GET'])
def tasa_crecimiento_ventas():
    try:
        query = "SELECT Fecha, Total FROM Ventas"
        df = pd.read_sql_query(query, postgres_connection)

        df['Fecha'] = pd.to_datetime(df['fecha'])
        
        df['Year'] = df['Fecha'].dt.year
        df['Month'] = df['Fecha'].dt.month
        
        df_grouped = df.groupby(['Year', 'Month']).agg(TotalVentas=('total', 'sum')).reset_index()
        
        df_grouped['Crecimiento'] = df_grouped['TotalVentas'].pct_change() * 100

        df_grouped['Crecimiento'] = df_grouped['Crecimiento'].fillna(0)
        
        result = df_grouped.to_dict(orient='records')
        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": "Error inesperado", "details": str(e)}), 500


def agendar_carga():
    # Se agenda la tarea para que se ejecute de lunes a sábado a las 03:00 AM
    schedule.every().monday.at("12:00").do(ETL.ejecutarCargaBatch)
    schedule.every().tuesday.at("12:00").do(ETL.ejecutarCargaBatch)
    schedule.every().wednesday.at("12:00").do(ETL.ejecutarCargaBatch)
    schedule.every().thursday.at("12:00").do(ETL.ejecutarCargaBatch)
    schedule.every().friday.at("12:00").do(ETL.ejecutarCargaBatch)
    schedule.every().saturday.at("15:00").do(ETL.ejecutarCargaBatch)

    while True:
        schedule.run_pending()
        time.sleep(1)

def iniciar_agendador():
    agendador = threading.Timer(0, agendar_carga)
    agendador.daemon = True
    agendador.start()

if __name__ == "__main__":
    iniciar_agendador()
    app.run(port=9001, debug=True)
