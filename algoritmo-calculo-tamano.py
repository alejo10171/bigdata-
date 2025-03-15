import time
import sys
import random
import psycopg2
from psycopg2 import Error

# Variables globales
error_con = False

# Parámetros de conexión de la Base de datos local
v_host = "localhost"
v_port = "5432"
v_database = "bigdata"
v_user = "postgres"
v_password = "postgres"

# Función: Cargar Operaciones
def cargarOperaciones(conn, cur, reg, dep, mun, prod, fec, cant):
    try:
        command = '''INSERT INTO tamanio (id_registro, id_departamento, id_municipio, 
                                          id_producto, fecha, cantidad, estado) 
                     VALUES (%s, %s, %s, %s, %s, %s, %s);'''
        cur.execute(command, (reg, dep, mun, prod, fec, cant, 'V'))
        conn.commit()
    except (Exception, Error) as error:
        print("Error en carga de operaciones:", error)
        sys.exit("Error: Carga Operaciones!")

# Conexión a la base de datos
try:
    connection = psycopg2.connect(user=v_user, password=v_password, host=v_host,
                                  port=v_port, database=v_database)
    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    print("Conectado a - ", cursor.fetchone())

    # Verificar si la tabla `municipios` tiene datos
    cursor.execute("SELECT COUNT(*) FROM municipios;")
    total_municipios = cursor.fetchone()[0]
    if total_municipios == 0:
        sys.exit("Error: La tabla 'municipios' está vacía. Inserta datos antes de continuar.")

    # Limpieza de la tabla
    cursor.execute("TRUNCATE tamanio;")    
    connection.commit()
except (Exception, Error) as error:
    print("Error de conexión:", error)
    error_con = True
finally:
    if error_con:            
        sys.exit("Error de conexión con PostgreSQL")

# Cantidades de registros a procesar
cantidades_registros = [10000, 100000, 1000000, 10000000]

# Ejecución para cada cantidad de registros
for registros in cantidades_registros:
    print(f"\nProcesando {registros} registros...")

    # Tiempo de inicio
    tiempo_inicio = time.time()

    for iteracion in range(1, registros + 1):
        id_producto = random.randint(1, 4)
        cantidad = random.randint(1, 5000)
        fecha = f"{random.randint(1, 28):02d}-{random.randint(1, 12):02d}-2023"

        # Obtener un municipio aleatorio con verificación
        cursor.execute("SELECT id_departamento, id_municipio FROM municipios ORDER BY RANDOM() LIMIT 1;")
        record = cursor.fetchone()

        if record:
            id_departamento, id_municipio = record
        else:
            print(f"Error en la iteración {iteracion}: No se encontraron registros en 'municipios'.")
            sys.exit("Error: La consulta a 'municipios' no devolvió datos.")

        # Cargar operación
        cargarOperaciones(connection, cursor, iteracion, id_departamento, id_municipio, id_producto, fecha, cantidad)

    # Tiempo de finalización
    tiempo_fin = time.time()
    tiempo_total = tiempo_fin - tiempo_inicio
    print(f"Tiempo de procesamiento para {registros} registros: {tiempo_total:.2f} segundos")

    # Tamaño de la base de datos
    cursor.execute("SELECT pg_size_pretty(pg_database_size('bigdata'));")
    print("Tamaño de la base de datos:", cursor.fetchone()[0])

    # Tamaño de la tabla "tamanio"
    cursor.execute('''SELECT pg_size_pretty(pg_total_relation_size('tamanio'));''')
    print("Tamaño de la tabla 'tamanio':", cursor.fetchone()[0])

connection.close()
print("\nConexión PostgreSQL cerrada")
print("\nProceso completado")
