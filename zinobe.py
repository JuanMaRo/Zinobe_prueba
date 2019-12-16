# imports

import requests
import pandas
import sqlite3
import json
import hashlib
import time

# from unicodedata import normalize
# Listas que serán los resultados de los puntos 1, 2, 3 y 4  para despues incluirlas en el data frame

# regions = ['africa', 'americas', 'asia', 'europe', 'oceania'] # Este debería ser el resultado del punto 1
# s = set()
# regions = list(s)



def get_regions():
    url = "https://restcountries-v1.p.rapidapi.com/all"

    headers = {
        'x-rapidapi-host': "restcountries-v1.p.rapidapi.com",
        'x-rapidapi-key': "Aqui-va-la-clave"
        }

    response = requests.request("GET", url, headers=headers).json()

    s = set()

    for i in range(len(response)):
        s.add(response[i]['region'].lower())

    return s


def encrypt(word):
    # palabra a binario y encriptada en sha1
    encode = hashlib.sha1(word.encode())
    # la palabra en valor hexadecimal
    value = encode.hexdigest()
    return value
    #print(value) # ESTE VALOR SERA INCLUIDO EN LA TABLA por medio de un append a una lista


def get_region_data(regions):
    url = 'https://restcountries.eu/rest/v2/region/'
    regions_urls = [url + region for region in regions]

    for url in regions_urls:
        start = time.time()

        # hice una peticion a al API y el resultado lo converti a JSON
        response_json = requests.get(url).json()

        # nombre del pais de la region
        country_name = response_json[0]['name'] # ESTE VALOR SERA INCLUIDO EN LA TABLA
        countries_name.append(country_name)

        # nombre de la lengua del pais
        language = response_json[0]['languages'][0]['name']
        languages.append(language)
        encrypt_language.append(str(encrypt(language)).upper())

        # el tiempo que toma en armar la fila calculado en segundos
        end = float(round((time.time() - start), 2))
        iteration_time.append(end)# ESTE VALOR SERA INCLUIDO EN LA TABLA


def pandas_data(regions, countries_name, encrypt_language, iteration_time_string):
    #el dataframe
    df = pandas.DataFrame({'Region': regions, 
                           'City Name': countries_name,
                           'Language': encrypt_language,
                           'Time': iteration_time_string}, 
                           columns=['Region', 'City Name', 'Language', 'Time'])
    
    return df


def pandas_series(iteration_time):
    series = pandas.Series(iteration_time)

    s = {}

    s['mean'] = series.mean() # promedio
    s['max'] = series.max() # el tiempo maximo
    s['min'] = series.min() # el tiempo minimo
    s['sum'] = series.sum() # el total del tiempo

    s = pandas.Series(s)
    return s


def dataframe_to_sql_to_json(df):
    # nombre de la base de datos
    connection = sqlite3.connect('zinobe.sqlite3')
    c = connection.cursor()

    # parametros de la base de datos y creacion
    c.execute('CREATE TABLE WORLD (Region text, City_Name text, Language text, Time text)')
    connection.commit()

    data_frame = df
    data_frame.to_sql('WORLD', connection, if_exists='replace', index=False)

    result = c.execute('SELECT * FROM WORLD')

    items = []

    for row in result:
        items.append({'region': row[0], 'country': row[1], 'language': row[2], 'time': row[3]})

    with open('data.json', 'w') as json_file:
        json.dump(items, json_file, indent=2, sort_keys=True)
        json_file.write('\n')


def series_to_sql_to_json(s):
    # nombre de la base de datos
    connection = sqlite3.connect('zinobe.sqlite3')
    c = connection.cursor()

    # parametros de la base de datos y creacion
    c.execute('CREATE TABLE TIME (Time text)')
    connection.commit()

    series = s
    series.to_sql('TIME', connection, if_exists='replace', index=False)

    keys = ['max', 'media', 'min', 'sum']
    values = []

    for i in series:
        values.append(i)

    # cree un diccionario con clave valor para hacer mas legible el json
    items = [dict(zip(keys, values))]

    # se agregan los resultados a data.json para crear un solo archivo
    with open('data.json', 'a+') as json_file:
        # json_file.write('\n Resultados del tiempo \n')
        json.dump(items, json_file, indent=2, sort_keys=True)
  

if __name__ == '__main__':
    
    regions = sorted(list(get_regions()))
    regions.remove('')

    countries_name = []
    languages = []
    encrypt_language =  []
    iteration_time = []

    get_region_data(regions)

    iteration_time_string = [str(i) + ' ms' for i in iteration_time]

    print(
        pandas_data(regions, countries_name, encrypt_language, iteration_time_string)
    )

    dataframe_to_sql_to_json(
        pandas_data(regions, countries_name, encrypt_language, iteration_time_string)
    )

    pandas_series(iteration_time)

    series_to_sql_to_json(pandas_series(iteration_time))
