import traceback
from util.db_connection import Db_Connection
import pandas as pd

def cargar_fact_inventory():
    try:
        # Conexión a la base de datos Staging
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'Tonygoku'
        db = 'staging'
        
        con_staging = Db_Connection(type, host, port, user, pwd, db)
        ses_staging = con_staging.start()
        if ses_staging == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_staging == -2:
            raise Exception("Error tratando de conectarse a la base de datos Staging")
        
        # Conexión a la base de datos OLTP
        db_oltp = 'oltp'
        con_oltp = Db_Connection(type, host, port, user, pwd, db_oltp)
        ses_oltp = con_oltp.start()
        
        # Consulta para extraer los datos de la tabla film desde OLTP
        films_query = "SELECT film_id, rental_rate, replacement_cost FROM film"
        films_oltp = pd.read_sql(films_query, ses_oltp)
        
        # Consulta para extraer los datos de inventory desde Staging
        inventory_query = "SELECT * FROM ext_inventory"
        inventory_staging = pd.read_sql(inventory_query, ses_staging)

        # Unir tablas (fact_inventory con las columnas adicionales de film)
        fact_inventory = pd.merge(inventory_staging, films_oltp, on='film_id', how='left')

        # Conexión a la base de datos Sor
        db_sor = 'sor'
        con_sor = Db_Connection(type, host, port, user, pwd, db_sor)
        ses_sor = con_sor.start()
        
        # Cargar los datos en la tabla fact_inventory de Sor
        fact_inventory.to_sql('fact_inventory', ses_sor, if_exists='replace', index=False)
        print("Datos cargados en Sor exitosamente.")

    except:
        traceback.print_exc()
    finally:
        pass
