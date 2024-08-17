import pandas as pd
from sqlalchemy import create_engine, exc

def ejecutarCargaBatch():
    try:
        sql_server_connection = create_engine(
            'mssql+pymssql://sa:1234@localhost:1433/BotanicStickDB'
        )
        postgres_connection = create_engine(
            'postgresql://postgres:root@localhost:5432/postgres'
        )

        tablas = [ 
            {'tabla':'Ventas','metodo':"append"},
            {'tabla':'DetallesVenta','metodo':"append"},
            {'tabla':'Productos','metodo':"append"},
            {'tabla':'Pedidos','metodo':"append"},
            {'tabla':'EstadosPedido','metodo':"replace"},
            {'tabla':'ProductoTerminado','metodo':"append"},
            {'tabla':'AspNetUsers','metodo':"replace"},
            {'tabla':'AspNetRoles','metodo':"replace"}
        ]
        batch_size = 1000

        for tabla_info in tablas:
            tabla = tabla_info['tabla']
            modo = tabla_info['metodo']

            query = f"SELECT * FROM {tabla}"
            batch_iter = pd.read_sql(query, sql_server_connection, chunksize=batch_size)

            for i, df in enumerate(batch_iter):
                df.to_sql(tabla, postgres_connection, if_exists=modo, index=False)

    except Exception as e:
        print(f"Error durante la carga de datos: {e}")
        raise e