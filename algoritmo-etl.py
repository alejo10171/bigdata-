# -*- coding: utf-8 -*-
"""
@author: jaimesoto
"""
# -*- coding: utf-8 -*-
"""
@Institución: IU Pascual Bravo
@author     : Jaime E Soto U
@asignatura : ET01555 - Fundamentos de Bigdata 
@grupo      : 100
@Tarea      : Tarea UNIDAD 2 - Procesamiento ETL (Extraction-Transformation-Load)
@Periodo    : 2024-2

Data de Colombia
https://www.datos.gov.co/Mapas-Nacionales/Departamentos-y-municipios-de-Colombia/xdk5-pm3f/data
"""

#-------------------------------------------------------------------------
# EN ESTE ALGORITMO
# - Incluir la actualización del campo "id_region" en la tabla
#   "operaciones".
# - Previo a eso, se debe crear una tabla regiones y modificar
#   la tabla "operaciones" para agregar el campo. Puede hacer esta
#   actividad manualmente o a través de un ALTER TABLE.
#-------------------------------------------------------------------------

import time
import sys
import re
import random
import pandas as pd
import psycopg2
from   psycopg2 import Error
import csv



csv_path = "C:\\Users\\user\\OneDrive\\Escritorio\\colombia-dane-departamentos.csv"
# Variables globales
error_con = False
id_pais   = 57      # codigo ISO Colombia


# Parámetros de conexión de la Base de datos local
v_host	   = "localhost"
v_port	   = "5432"
v_database = "bigdata"
v_user	   = "postgres"
v_password = "postgres"

#-----------------------------------------------------------------------------
# Función: Obtener número de código de Región
#-----------------------------------------------------------------------------
def getCodigoRegion(region):
    codigo_region = 0
    if region == "Región Eje Cafetero - Antioquia":
        codigo_region = 1
    elif region == "Región Centro Oriente":
         codigo_region = 2
    elif region == "Región Centro Sur":
         codigo_region = 3
    elif region == "Región Caribe":
         codigo_region = 4
    elif region == "Región Llano":
         codigo_region = 5
    elif region == "Región Pacífico":
         codigo_region = 6
    else:
         codigo_region = 0
    return codigo_region    
    # Fin función getCodigoRegion


#-----------------------------------------------------------------------------
# Función:  Carga Tabla Temporal
#-----------------------------------------------------------------------------
def cargarTablaTemporal(conn, cur, contador, region, codigo_region, codigo_dep, departamento, 
                        codigo_mun, municipio):
    print("Cargando temporal ... -> ", len(codigo_mun) , contador, codigo_dep, codigo_mun, departamento, 
          municipio, codigo_region, region)

    if (len(codigo_mun) > 10):
        print("Problemas con el código de departamento ... fila -> ",contador+1)
        return
    try:
        # Comando SQL
        command = '''INSERT INTO temporal(codigo_region, codigo_dep, codigo_mun, departamento, municipio, region) 
                     VALUES (%s,%s,%s,%s,%s,%s);'''
        cur.execute(command, (codigo_region, codigo_dep, codigo_mun, departamento, municipio, region))
        conn.commit()
    except (Exception, Error) as error:
        print("Error: ", error)
        sys.exit("Error: Carga tabla temporal!")
    finally:
        return
    # Fin función cargarTablaTemporal


#----------------------------------------------------------------------------------
# Clase:  Cargar Departamentos (desde la información obtenida en la tabla temporal)
#----------------------------------------------------------------------------------
def cargarDepartamento(conn, cur, contador, codigo_pais, codigo_dep, dapartamento, codigo_region,
                       cantidad):
    sufijo          = str(codigo_dep).zfill(2)
    id_departamento = int(str(id_pais) + sufijo)    
    abb             = ''
    nombre          = departamento[0:50]
    print(sufijo, id_departamento, nombre, codigo_dep, codigo_region, cantidad)
    # Comando SQL
    command = '''INSERT INTO departamentos (id_departamento, codigo_dane, nombre, abb, codigo_region) 
                 VALUES (%s,%s,%s,%s,%s);'''
    cur.execute(command, (id_departamento, codigo_dep, nombre, abb, codigo_region))
    conn.commit()
    return
    # Fin función cargarDepartamento
    

#-----------------------------------------------------------------------------
# Clase:  Cargar Municipios (desde la información obtenida en la tabla temporal)
#-----------------------------------------------------------------------------
def cargarMunicipio(conn, cur, contador, id_pais, codigo_dep, codigo_mun, municipio):
    try:
        sufijo          = str(codigo_dep).zfill(2)
        id_departamento = int(str(id_pais) + sufijo)    
        sufijo          = str(contador).zfill(3)
        id_municipio    = int(str(id_departamento) + sufijo)    
        nombre          = municipio[0:50]
        abb             = ''
        print(contador, sufijo, id_departamento, id_municipio, nombre, codigo_mun, end="\n") 
    
        command = '''INSERT INTO municipios (id_departamento, id_municipio, nombre, codigo_dane, abb) 
                     VALUES (%s,%s,%s,%s,%s);'''
        cur.execute(command, (id_departamento, id_municipio, nombre, codigo_mun, abb))
        conn.commit()
    except (Exception, Error) as error:
        print("Error en carga de Municipios: ", error)
        sys.exit("Error: Carga Municipios!")
    finally:
        return    
    # Fin función cargarMunicipio

         
# --------------------------------------------------------------------------
# INICIO DEL PROGRAMA
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# CONEXIÓN A LA BASE DE DATOS
# --------------------------------------------------------------------------
try:
    # Conexión local a la base de datos
    # Se utilizan los parámetros valorizados al inicio del programa
    connection = psycopg2.connect(user= v_user, password=v_password, host= v_host,
                                  port= v_port, database= v_database)
    # Creación cursor para realizar operaciones en la basedatos
    cursor = connection.cursor()
    # Ejecución de SQL query
    cursor.execute("SELECT version();")
    # Fetch result
    record = cursor.fetchone()
        # Imprime detalles de PostgreSQL
    print("PostgreSQL Información del Servidor")
    print(connection.get_dsn_parameters(), "\n")    
    print("Python version: ",sys.version)    
    print("Estás conectado a - ", record, "\n")
    print("Base de datos:", v_database, "\n")
    # -------------------------------------------------------------------------
    # LIMPIEZA DE TABLAS
    # instrucción "TRUNCATE" realiza la misma operación que DELETE FROM "table"
    # -------------------------------------------------------------------------
    command = '''TRUNCATE temporal;'''
    cursor.execute(command)    
    command = '''TRUNCATE departamentos;'''
    cursor.execute(command)
    command = '''TRUNCATE municipios;'''
    cursor.execute(command)
    connection.commit()    
except (Exception, Error) as error:
    print("Error: ", error)
    error_con = True
finally:
    if (error_con):            
        sys.exit("Error de conexión con servidor PostgreSQL")



# ------------------------------------------------------------------------- 
# Extracción de Datos - Hoja de Cálculo (CSV)
# Carga en archivo temporal de la base de datos
# ------------------------------------------------------------------------- 
try:
    # CABECERA ARCHIVO CSV
    # REGION, CODIGO DANE DEPARTAMENTO, DEPARTAMENTO, CODIGO DANE MUNICIPIO, MUNICIPIO    
    #   region              = row[0] - Primer dato a la izquierda tiene índice "0"
    #   codigo_departamento = row[1]
    #   departamento        = row[2]
    #   codigo_municipio    = row[3]
    #   municipio           = row[4]    
    #
    archivo    = 'colombia-dane-departamentos.csv'

    # Recorrido/Lectura de los registros de la hoja de cálculo
    with open(archivo) as File:
        reader = csv.reader(File, delimiter=',', quotechar=',', quoting=csv.QUOTE_MINIMAL)   
        #
        contador = 0
        for row in reader:
            # ---------------------------------------------------------------------
            # Saltar la cabecera de la hoja de cálculo
            # No se toma en cuenta las cabeceras de descripción de las columnas
            # ---------------------------------------------------------------------
            if (contador > 0):
                region        = row[0]
                codigo_region = getCodigoRegion(region)  
                # ---------------------------------------------------------------------                
                # Carga de tabla temporal
                # Esta tabla servirá de pivote para cargar departamentos y municipios
                # posteriormente
                # ---------------------------------------------------------------------
                cargarTablaTemporal(connection, cursor, contador, region, codigo_region,
                                    row[1], row[2], row[3], row[4])
            # Fin ciclo de recorrido de la hoja de cálculo
            contador = contador + 1
except (Exception, Error) as error:
    print("Error (excepción): ", error)
    sys.exit("Error ->  Fase Excel")
finally:
    File.close()
    print("Tabla Temporal cargada ....")




# --------------------------------------------------------------------------
# CUERPO PRINCIPAL DEL PROGRAMA - PROCESAMIENTO DE DEPARTAMENTOS
# --------------------------------------------------------------------------
try:
    # Código de carga en tablas
    print("Inicia la carga de información ...")

    # -------------------------------------------------------------------------
    # DEPARTAMENTOS 
    # -------------------------------------------------------------------------    
    print("DEPARTAMENTOS")    
    # ATENCIÓN: Ponga especial atención en la select y el uso de "distinct"
    # Agrupamiento/Ordenamiento tabla temporal
    # El resultado de la consulta es el agrupamiento de los departamentos
    sql = '''SELECT distinct codigo_dep, departamento, codigo_region, count(*) as veces from temporal 
            group by codigo_dep, departamento, codigo_region order by departamento'''
    cursor.execute(sql)
    registros = cursor.fetchall()
    # Recorrido de todos los registros de la tabla temporal
    contador   = 0
    for row in registros:
        contador       = contador + 1
        codigo_dep     = row[0] # Primer campo de la selección
        departamento   = row[1]
        codigo_region  = row[2]
        cantidad       = row[3]
        # Pasaje de parámetros y carga en tabla "departamentos"
        cargarDepartamento(connection,cursor,contador,id_pais,codigo_dep,departamento,codigo_region,cantidad) 
    # -------------------------------------------------------------------------
        
    # -------------------------------------------------------------------------
    # MUNICIPIOS        
    # -------------------------------------------------------------------------
    print("MUNICIPIOS")    
    sql = '''SELECT codigo_dep, codigo_mun, municipio from temporal order by departamento, municipio'''
    cursor.execute(sql)    
    registros = cursor.fetchall()
    # Recorrido de todos los registros de la tabla temporal
    contador   = 0
    codigo_old = ''
    for row in registros:
        codigo_dep  = row[0]
        codigo_mun  = row[1]
        municipio   = row[2]
        # Resetea el contador para un nuevo departamento
        if (codigo_dep == codigo_old):
            contador   = contador + 1
        else:
            contador   = 1
        codigo_old = codigo_dep
        #
        cargarMunicipio(connection, cursor, contador, id_pais, codigo_dep, 
                           codigo_mun, municipio) 
    # -------------------------------------------------------------------------        

    # -------------------------------------------------------------------------
    connection.commit()
    connection.close()    
    
except (Exception, Error) as error:
    print("Error de procesamiento de la tabla!", error)
finally:
    if (connection):
        connection.close()
        print("Conexión PostgreSQL cerrada")    
        
print("Fin del proceso ETL")
# Fin del algoritmo
