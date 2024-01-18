#this etl script imports the historic data from postgress to monitored collection
#import the packages
import psycopg2
import os,sys
import configparser
import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from parameters.get_conection import *
from datetime import datetime, timedelta
from ormWP import Waterpoint
from mongoengine import connect
#connection to the mongo db


#connection to the postgres and return the dataframe
#retrive location data and return the dataframe for both monitoring and climatology
#the function has the argument wp_indb to filter the table from postgres table location_data based on the existing waterpoints in mongodb
water=requests.get('https://webapi.waterpointsmonitoring.net/api/v1/waterpoints').json()
wp_indb = [int(item['ext_id']) for item in water]


def get_wpmonitored(wp_indb):
    print("Ejecutando monitoreo")
    conn = None
    try:
        conn = psycopg2.connect(get_postres_conn_str())
        cur = conn.cursor()

    except Exception as ex:
        print('Error en la conexión:')
        print(str(ex))

    # Calcular la fecha tres días después del 22 de diciembre de 2021
    

    sql = f"select location_id, date, day, rain, evap, depth, scaled_depth from location_data where location_id=ANY (select uid from locations where country='Ethiopia' and  uid in {tuple(wp_indb)});"
    cur.execute(sql)
    results = cur.fetchall()

    # Filtrar las columnas requeridas y devolver el DataFrame de pandas
    df = pd.DataFrame(results, columns=['location_id', 'date', 'day', 'rain', 'evap', 'depth', 'scaled_depth'])
    df['date'] = pd.to_datetime(df['date'])
    fecha_referencia = datetime.now()
    tres_dias_antes = fecha_referencia - timedelta(days=3)
    print(tres_dias_antes)
    # Filtrar el DataFrame para los tres días después del 22 de diciembre de 2021
    tres_dias_data = df[df['date'] >= tres_dias_antes]
    print(tres_dias_data)
    json_data = tres_dias_data.to_json(orient='records')
    print(json_data)

    # Hacer una solicitud POST con el encabezado del token de portador y los datos JSON
    url = 'http://localhost:5000/api/v1/monitored/dialy_update'
    headers = {
        'Content-type': 'application/json',
        'Authorization': 'Bearer prueba'  # Reemplazar 'prueba' con tu token real
    }
    response = requests.post(url, data=json_data, headers=headers)
    print(response.status_code)
    print(response.text)

    return json_data

get_wpmonitored(wp_indb)




def get_wpclimatology(wp_indb):
    print("running climatology")
    conn = None
    try:
        conn = psycopg2.connect(get_postres_conn_str())
        cur = conn.cursor()

    except Exception as ex:
        print('error in connection:')
        print(str(ex))


    sql=f"select location_id,date,day,rain,evap,depth,scaled_depth from location_data where location_id=ANY (select uid from locations where country='Ethiopia' and  uid in {tuple(wp_indb)});"
    cur.execute(sql)
    results = cur.fetchall()
   
    # filter the requreid columns and return the pandas dataframe
    df = pd.DataFrame(results, columns=['location_id', 'date', 'month_day', 'rain', 'evap', 'depth', 'scaled_depth'])
    json_data = df.to_json(orient='records')
#make a post ,method, with header baerer token and json data
    
    url = 'http://localhost:5000/api/v1/monitored/update_climatology'
    headers = {
    'Content-type': 'application/json',
    'Authorization': 'Bearer prueba'  
}
    response = requests.post(url, data=json_data, headers=headers)
    print(response.status_code)
    print(response.text)

    return json_data
get_wpclimatology(wp_indb)
#this is the main function for the import process and comments are included for each methods used in the script building
