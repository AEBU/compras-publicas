# Librerías airflow

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Librerías auxiliares
import requests
import json 
import zipfile
import wget
from datetime import date
from random import random
from pyspark.sql.functions import from_json, col, explode, explode_outer, size, array_contains
from pyspark.sql import DataFrame, SparkSession
from typing import List
import pyspark.sql.types as T
import pyspark.sql.functions as F
import pandas as pd
import numpy as np

#Librarías para base de datos
from py2neo import Graph
from py2neo import Node, Relationship

#configuracion base de datos
uri =  "bolt://XXX.XX.XXX.XX:7687"
graph = Graph(uri, auth=("neo4j", "XXXXXXXXX"))


## Constantes
NAME_FILE = 'datos_abiertos.zip'
PATH_DOWNLOAD_RAW = f'/XXXXXX/datalake/1_RAW/source_sercop/analytics/json'
PATH_DOWNLOAD_RAW_FILE = f'{PATH_DOWNLOAD_RAW}{NAME_FILE}'
NAME_FILE_DWN = 'releases_2022_enero.json'

# DAG
# ------------------------------>

#TASKS pertenecen a un DAG:

default_args = {
    'owner': 'Alexis B.',
    'start_date': days_ago(1),
}

dag_args = {
    'dag_id': 'compras_publicas',
    'schedule_interval': '@montly',
    'catchup': False,
    'default_args': default_args
}


with DAG( **dag_args ) as dag:
    
    def mapping_data(x):
        """
        Se crea el mapeo de datos del json descargado de parte de compras públicas.
        """
        data_supercias = {}
        # Buyer
        if ( 'buyer' in x.roles):
            buyer = {
                'id' : x.id_entidad,
                'name' : x.name_entidad,
                'ruc' : x.id_entidad.split('-')[2]
            }
            data_supercias['buyer'] = buyer

        #Supplier
        if ( 'supplier' in x.roles ):
            supplier = {
                'id' : x.id_entidad,
                'name' : x.name_entidad,
                'ruc' : x.id_entidad.split('-')[2]
            }
            data_supercias['supplier'] = supplier

        # Ubicación
        data_supercias['location'] = {
            'country': x.countryName,
            'street': x.streetAddress.replace('s/n',''),
            'region': x.region,
            'locality': x.locality,

        }
        # Contrato
        data_supercias['contract'] = {
            'id_contract': x['releases.contracts'][0].id,
            'id_tender': x['releases.tender.id'],
            'order_name': x['releases.tender.title'],
            'amount_tender': x['amount_tender']   
        }

        # Producto
        data_supercias['product'] = {
            'id_item': x['id_item'][0],
            'quantity': x['quantity'][0],
            'amount_unitary': x['amount_unitary'][0],
            'description': x['description'][0],
            'classification_id': x['classification_id'][0]  
        }
        return (data_supercias)


    def get_data_file():
        """
        Permite descargar los datos (Por el momento un mes por default)
        """
        YEAR = 2022
        TYPE = 'json'
        MONTH = 1
        URL = f'https://datosabiertos.compraspublicas.gob.ec/XXXXXX/download?type=${TYPE}&year={YEAR}&month={MONTH}&method=all'
        file_name = wget.download(url= URL, out=f'{PATH_DOWNLOAD_RAW_FILE}' )
        print(f'file download {file_name}')


    def unzip_file():
        """
        Descomprime un archivo y lo coloca en su respectivo directorio.
        """
        print(f'download {PATH_DOWNLOAD_RAW_FILE}')
        print(f'rAW {PATH_DOWNLOAD_RAW}')
        with zipfile.ZipFile(PATH_DOWNLOAD_RAW_FILE, 'r') as zip_ref:
            print(zip_ref)
            zip_ref.extractall(PATH_DOWNLOAD_RAW)        


    def init_spark():
        """
        Se crea la sesión de spark y se crea  su respectivo análisis con PySpark
        """
        print(f'download {PATH_DOWNLOAD_RAW_FILE}')
        spark = SparkSession \
            .builder \
            .appName("Example") \
            .getOrCreate() 
        
        # Lectura del archivo Json
        df = spark.read.json(f'{PATH_DOWNLOAD_RAW}{NAME_FILE_DWN}', multiLine=True)
        
        # Se hace un explode del campo que tiene la información
        df_1 = df.select( col('releases.awards'),explode('releases').alias('releases') )\
                .where(array_contains(col('releases.tag'),'award') )
        
        # Se obtiene los datos de cada columna para las diferentes entidades
        df_2 = df_1.select(col("releases.date").alias("fecha_contrato"), col("releases.ocid").alias("ocid"), col("releases.tag").alias("etapas"), \
            explode('releases.parties').alias('parties'),\
            col('releases.awards').alias('awards'),\
            col('parties.id').alias('id_entidad'), col('parties.name').alias('name_entidad'), col('parties.roles'), 
            col('parties.address.countryName'), col('parties.address.streetAddress'), col('parties.address.region'), col('parties.address.locality'),\
            col('releases.contracts'), \
            col('releases.tender.id'), col('releases.tender.title'), col('releases.tender.status'), col('releases.tender.description'), col('releases.tender.procurementMethodDetails'), col('releases.tender.value.amount').alias('amount_tender')  )\
            .where ( array_contains('parties.roles', 'supplier') | array_contains('parties.roles', 'buyer')  )
        
        # Datos del producto
        df_3 = df_2.select(explode('awards').alias('product'),\
                   col('product.id'), col('product.value.amount').alias('amount_product'),\
                   col('product.items'),\
                   col("*")
                   )
        df_4 = df_3.select(explode('items').alias('items'),\
                   col("items.id").alias('id_item'),col("items.quantity"),col("items.description").alias('items_descrip'),\
                   col("items.classification.id").alias('classification_id'),col("items.classification.description"),\
                   col("items.unit.value.amount").alias('amount_unitary'),\
                   col("*")
                   )
        print('Preparación raelizada correctamente')
        # df_4.show(10)
        rdd2=df_4.rdd.map(lambda x:  
                  (mapping_data(x))
        ) 
        for x in rdd2:
            # ContratO
            y = x['contract']
            contract =  Node('CONTRACT', id_contract = y['id_contract'], id_tender = y['id_tender'], 
                            region = y['order_name'], locality=['amount_tender']  )
            graph.merge(contract, 'CONTRACT', 'id_contract') 

            # PRODUCTO
            y = x['product']
            producto =  Node('PRODUCT', id_item = y['id_item'], 
                            quantity = y['quantity'], amount_unitary=['amount_unitary'],
                            description = y['description'], classification_id=['classification_id'],
                            )
            graph.merge(producto, 'PRODUCT', 'id_item')
            
            #PRODUCTO RELATIONSHIP
            product_rel = Relationship(contract, "PRODUCT_REL",producto) 
            graph.merge(product_rel)  
            

            # Buyer  
            if ('buyer' in x):
                # print('buyer',x['buyer'])
                y = x['buyer']
                entity = Node('ENTIDAD', id = y['id'], name = y['name'], ruc = y['ruc']  )
                graph.merge(entity, 'ENTIDAD', 'id')
            
                #cONTRACT RELATIONSHIP
                contract_rel = Relationship(entity, "REQUIRED",contract) 
                graph.merge(contract_rel)  
            # Supplier
            if ('supplier' in x):
                print('supplier',x)
                y = x['supplier']
                entity = Node('ENTIDAD', id = y['id'], name = y['name'], ruc = y['ruc']  )
                graph.merge(entity, 'ENTIDAD', 'id') 
                #cONTRACT RELATIONSHIP
                contract_rel = Relationship(contract, "AWARDED",entity) 
                graph.merge(location_rel)    
            
            # Location
            y = x['location']
            location = Node('LOCATION', country = y['country'], street = y['street'], region = y['region'], locality=['locality']  )
            graph.merge(location, 'LOCATION', 'region') 
            
            location_rel = Relationship(entity, "LOCATED_IN",location) 
            graph.merge(location_rel)


    # Operadores a crear.

    download_data = PythonOperator(
        task_id = 'download_data',
        python_callable =  get_data_file
    )

    unzip_data = PythonOperator(
        task_id = 'unzip_data',
        python_callable =  unzip_file
    )


    init_spark_ = PythonOperator(
        task_id = 'init_spark',
        python_callable =  init_spark
    )




#Dependencias
download_data >> unzip_data >> init_spark_
    