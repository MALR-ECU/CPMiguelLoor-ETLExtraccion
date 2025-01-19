import logging
import pymssql
import os

sql_server = os.environ["SQL_SERVER"]  # Dirección del servidor SQL
sql_username = os.environ["SQL_USERNAME"]  # Usuario de SQL Server
sql_password = os.environ["SQL_PASSWORD"]  # Contraseña de SQL Server
sql_table_string = os.environ["SQL_TABLE_LANDING"]
sql_database, schema_name, table_name = sql_table_string.split('.') 

def insert_into_sql(df):
    try:
        with pymssql.connect(
            server=sql_server,
            user=sql_username,
            password=sql_password,
            database=sql_database
        ) as conn:
            cursor = conn.cursor()

            # Crear la tabla si no existe
            create_table_query = f"""
            USE {sql_database};
            IF NOT EXISTS (
                SELECT * 
                FROM {sql_database}.sys.tables t
                INNER JOIN {sql_database}.sys.schemas s ON t.schema_id = s.schema_id
                WHERE t.name = '{table_name}' AND s.name = '{schema_name}'
            )
            CREATE TABLE {sql_table_string} (
                [ID] INT IDENTITY(1,1) PRIMARY KEY,
                [Operador] VARCHAR(MAX) NULL,
                [Equipo] INT NULL,
                [Turno] INT NULL,
                [Conexion] VARCHAR(MAX) NULL,
                [Diametro] VARCHAR(MAX) NULL,
                [Orden de Produccion] INT NULL,
                [Lado] VARCHAR(50) NULL,
                [Colada] INT NULL,
                [Codigo Unico] NVARCHAR(MAX) NULL,
                [Varicion de Diametro] FLOAT(53) NULL,
                [Ovalidad] FLOAT(53) NULL,
                [Paso] FLOAT(53) NULL,
                [Conicidad] FLOAT(53) NULL,
                [Longitud de rosca] FLOAT(53) NULL,
                [Altura de rosca] FLOAT(53) NULL,
                [Perfil de Rosca] VARCHAR(50) NULL,
                [Espesor de cara] VARCHAR(50) NULL,
                [Estado] VARCHAR(50) NULL,
                [Motivo Descarte] VARCHAR(50) NULL,
                [Comentario] VARCHAR(MAX) NULL,
                [Month] INT NULL,
                [Day] INT NULL,
                [Year] INT NULL,
                [Hour] TIME NULL,
                [CreatedAt] DATETIME DEFAULT GETDATE()
            )
            """
            cursor.execute(create_table_query)
            conn.commit()
            logging.info(f"Tabla {sql_table_string} verificada/creada.")

            # Insertar los datos
            for _, row in df.iterrows():
                insert_query = f"""
                INSERT INTO {sql_table_string} (
                    Operador, Equipo, Turno, Conexion, Diametro, [Orden de Produccion], Lado, Colada, [Codigo Unico],
                    [Varicion de Diametro], Ovalidad, Paso, Conicidad, [Longitud de rosca], [Altura de rosca], [Perfil de Rosca],
                    [Espesor de cara], Estado, [Motivo Descarte], Comentario, Month, Day, Year, Hour
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """
                cursor.execute(insert_query, (
                    row.get("Operador"), row.get("Equipo"), row.get("Turno"), row.get("Conexion"),
                    row.get("Diametro"), row.get("Orden de Produccion"), row.get("Lado"), row.get("Colada"),
                    row.get("Codigo Unico"), row.get("Varicion de Diametro"), row.get("Ovalidad"), row.get("Paso"),
                    row.get("Conicidad"), row.get("Longitud de rosca"), row.get("Altura de rosca"),
                    row.get("Perfil de Rosca"), row.get("Espesor de cara"), row.get("Estado"),
                    row.get("Motivo Descarte"), row.get("Comentario"), row["Month"], row["Day"], row["Year"], row["Hour"]
                ))
            conn.commit()
            logging.info("Datos insertados en SQL Server correctamente.")
    except Exception as e:
        logging.error(f"Error al insertar en SQL Server: {e}")
        raise