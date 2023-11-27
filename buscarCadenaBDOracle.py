import cx_Oracle

# Conexión a la base de datos Oracle
def conectar_bd(config):    
    return cx_Oracle.connect(
            user=config['user'],
            password=config['password'],
            dsn=f"{config['host']}:{config['port']}/{config['service_name']}"
        )

# Función para buscar una cadena en todas las tablas
def buscar_cadena_en_tablas(conexion, cadena_busqueda):
    cursor = conexion.cursor()

    # Obtener todas las tablas del usuario actual
    cursor.execute('''
    SELECT a.table_name
    FROM user_tab_cols a
    WHERE a.data_type LIKE '%VARCHAR%'
    ''')
    
    tablas = [tabla[0] for tabla in cursor.fetchall()]
    #print(tablas)
    
    for tabla in tablas:
        #print(tabla)
        cursor.execute(f"SELECT a.column_name FROM user_tab_cols a WHERE a.table_name='{tabla}' and a.data_type LIKE '%VARCHAR%'")
        columnas = [linea[0] for linea in cursor.fetchall()]
        query = f"SELECT * FROM {tabla} WHERE "

        for columna in columnas:
            query += f"UPPER({columna}) LIKE UPPER('%{cadena_busqueda}%') OR "
        query = query[:-4]  # Eliminar el último 'OR'

        #print(query)

        cursor.execute(query)
        filas = cursor.fetchall()

        if filas:
            print("------------------------------------------")
            print(query)
            print(f"Se encontró la cadena en la tabla '{tabla}' en alguna de las columnas {', '.join(columnas)}")
            print("\n")
            print(filas)
            print("------------------------------------------")
            
    cursor.close()



bases_de_datos = [
    {
        'host': 'servername.com',
        'user': 'userNameXXX',
        'password': 'passwordXXX',
        'service_name': 'SERVICE_XXX',
        'port': '1555'
    },
    {
        'host': 'servername.com',
        'user': 'userNameXXX',
        'password': 'passwordXXX',
        'service_name': 'SERVICE_XXX',
        'port': '1555'
    },
    {
        'host': 'servername.com',
        'user': 'userNameXXX',
        'password': 'passwordXXX',
        'service_name': 'SERVICE_XXX',
        'port': '1555'
    }
    ]

cadena_busqueda = 'CadenaBuscada_XXX'

# Realizar la conexión a la base de datos y buscar la cadena en las tablas
try:
    for config in bases_de_datos:
        conexion = conectar_bd(config)
        buscar_cadena_en_tablas(conexion, cadena_busqueda)
        conexion.close()
        print("Búsqueda completada.")
    
except cx_Oracle.DatabaseError as e:
    print("Error al conectar a la base de datos:", e)
    raise
