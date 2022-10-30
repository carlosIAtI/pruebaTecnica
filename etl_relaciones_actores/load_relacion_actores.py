
import pandas as pd
import os
from sqlalchemy import create_engine
from urllib.parse import quote
import os
from dotenv import load_dotenv
load_dotenv()



def extraer_datos_excel():
    print(":::::::::::::::::::::::::")
    print("Inicia extraccion")
    source_file_name = os.getcwd()
    filename = "Matriz_de_adyacencia_data_team.xlsx"
    path_attch = os.path.join(source_file_name, 'etl_relaciones_actores', filename)
    xls = pd.ExcelFile(f"{path_attch}")
    print(":::::::::::::::::::::::::")
    print("Extraccion completada")
    return xls

def transform_datos_excel(xls):
    print(":::::::::::::::::::::::::")
    print("Inicia transformación")
    df = pd.read_excel(xls, 'Matriz de adyacencia')
    df_list_actores = pd.read_excel(xls, 'Lista de actores')
    df_list_actores.drop([0], inplace=True)
    df_list_actores.drop([1], inplace=True)
    df_list_actores.drop([2], inplace=True)
    df_list_actores = df_list_actores['Unnamed: 2'] 
    df_relacion_actores = pd.DataFrame(columns=[])
    list_actores = df_list_actores.values.tolist()
    df.pop('Unnamed: 0')
    df.pop('Unnamed: 1')
    df.drop([0], inplace=True)
    df.reset_index(drop=True, inplace=True)
    list_head = list(df.columns)
    df.set_axis(list_head, axis=1, inplace=True)
    for col in  df.columns:
        for index, row in df.iterrows():
            if df[col][index] == 1 :
                new_row = {'actor_1':list_actores[col-1], 'actor_2':list_actores[index]}
                df_relacion_actores = df_relacion_actores.append(new_row, ignore_index=True)
    print(":::::::::::::::::::::::::")
    print("Transformación completada")
    return df_relacion_actores

def load_relaciones_actores():
    df = extraer_datos_excel()
    df_relacion_actores = transform_datos_excel(df)
    print(":::::::::::::::::::::::::")
    print("load_relaciones_actores")
    host_mysql = os.getenv('HOST_MYSQL')
    port_mysql = os.getenv('PORT_MYSQL')
    user_mysql = os.getenv('USER_MYSQL')
    pass_mysql = os.getenv('PASS_MYSQL')
    schema = os.getenv('SCHEMA_MYSQL')
    tabla  = 'relacion_actores'
    engine = create_engine(f'mysql+mysqlconnector://{user_mysql}:%s@{host_mysql}:{port_mysql}/{schema}' % quote(pass_mysql))

    try:
        df_relacion_actores.to_sql(name=tabla, con=engine, if_exists='replace', index=False,chunksize=10000)
    except Exception as e:
        print(e)
    pass