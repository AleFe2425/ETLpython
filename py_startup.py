# this file is a kind of python startup module used for manual unit testing

#from util.db_connection import Db_Connection
from extract.ext_countries import extraer_countries
from extract.ext_stores import extraer_stores
from extract.per_staging import persistir_staging
from transform.tra_stores import transformar_stores
from load.load_stores import cargar_stores
from extract.ext_address import extraer_address
from extract.ext_film import extraer_film
from extract.ext_date import extraer_date
from extract.ext_inventory import extraer_inventory
from extract.ext_city import extraer_city
from transform.tra_film import transformar_films
from extract.per_stagingaddress import persistir_staging_address
from load.load_fact_inventory import cargar_fact_inventory
import traceback
import pandas as pd

try:
    #con_db = Db_Connection('mysql','localhost','3306','root','Tonygoku','oltp')
    #ses_db = con_db.start()
    #if ses_db == -1:
    #    raise Exception("El tipo de base de datos dado no es válido")
    #elif ses_db == -2:
    #    raise Exception("Error tratando de conectarse a la base de datos ")
    
    #databases = pd.read_sql ('show databases', ses_db)
    #print (databases)
    
    #countries
    print("Extrayendo datos de countries desde un CSV")
    countries = extraer_countries()
    #print (countries)
    print("Persistiendo en Staging datos de countries")
    persistir_staging(countries,'ext_country')
    
    #store
    print("Extrayendo datos de stores desde una OTLP")
    stores = extraer_stores()
    #print (stores)
    print("Persistiendo en Staging datos de stores")
    persistir_staging(stores,'ext_store')
    print("Transformando datos de stores en el Staging")
    tra_stores = transformar_stores()
    #print (tra_stores)  
    print("Persistiendo en Staging datos transformados de stores")
    persistir_staging(tra_stores,'tra_store')
    print("Cargando datos de stores en SOR")
    cargar_stores()
    
    #address
    print("Extrayendo datos de addresses desde OLTP")
    address = extraer_address()
    print (address)
    print("Persistiendo en Staging datos de addresses")
    persistir_staging_address(address)
    
    #film
    print("Extrayendo datos de film desde OLTP")
    film = extraer_film()
    print(film)
    print("Persistiendo en Staging datos de film")
    persistir_staging(film,'ext_film')
    print("Transformando datos de films en el Staging")
    transformar_films = transformar_films()
    print (transformar_films)
    
    #date
    print("Extrayendo datos de date desde OLTP")
    dates = extraer_date()
    print(dates)
    print("Persistiendo en Staging datos de date")
    persistir_staging(dates,'ext_date')
    
    #inventory
    print("Extrayendo datos de inventory desde OLTP")
    inventory = extraer_inventory()
    print(inventory)
    print("Persistiendo en Staging datos de inventory")
    persistir_staging(inventory,'ext_inventory')
    
    #city
    print("Extrayendo datos de cities desde OLTP")
    city = extraer_city()
    print(city)
    print("Persistiendo en Staging datos de cities")
    persistir_staging(city,'ext_city')

    print("Cargando datos de fact_inventory en Sor")
    cargar_fact_inventory()

except:
    traceback.print_exc()
finally:
    pass
    #ses_db_oltp = con_db.stop()