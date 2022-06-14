import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, StringType,sha2
from pyspark.sql.types import DecimalType,StringType 
from pyspark.sql import functions as f
import sys
import boto3
import pyspark.sql.utils  
from pyspark.sql.utils import AnalysisException


spark = SparkSession.builder.appName("Transformation").getOrCreate()
sc = spark.sparkContext

spark.sparkContext.addPyFile("s3://aiswarya-landingzone/Files/delta-core_2.12-0.8.0.jar")

from delta import *

class Configuration:
    def __init__(self,spark_config_path,datasetName,dataset_path):

        self.s3 = boto3.resource('s3')
        
        self.setSparkConfig(spark_config_path)
        self.jsonData = self.read_config()
        
        self.raw_source = self.jsonData['masked-actives']['source']['data-location']+dataset_path
        self.raw_destination = self.jsonData['masked-actives']['destination']['data-location']
        
        self.raw_source_format = self.jsonData['masked-actives']['source']['file-format']
        self.raw_destination_format = self.jsonData['masked-actives']['destination']['file-format']

        
        self.masking_columns = self.jsonData['masked-actives']['masking-cols']
        self.transformation_columns = self.jsonData['masked-actives']['transformation-cols']  
  
        self.lookup_location = self.jsonData['lookup-dataset']['data-location']
        self.pii_cols = self.jsonData['lookup-dataset']['pii-cols']
        
        self.partition_columns = self.jsonData['masked-actives']['partition-cols'] 
        
    def setSparkConfig(self,location):
        location_list = location.replace(":","").split("/")
        
        obj = self.s3.Object(location_list[2], "/".join(location_list[3:]))
        body = obj.get()['Body'].read()
        list_raw = json.loads(body)
        spark_properties = list_raw[0]['properties']
        list_updated = list()
        
        for i in spark_properties:
            list_updated.append((i,spark_properties[i]))
            
        conf = spark.sparkContext._conf.setAll(list_updated)
    
    #Configuration
    def read_config(self):
        path = spark.sparkContext._conf.get('spark.path')
        path_list = path.replace(":","").split("/")
        obj = self.s3.Object(path_list[2], "/".join(path_list[3:]))
        body = obj.get()['Body'].read()
        jsondata = json.loads(body)
        print("App Config Read")
        return jsondata
    
class Transformation:
    def reading_data(self, path,format):
        try:
            if format == "parquet":
                df=spark.read.parquet(path)
                print("Data Read")
                return df
            elif format == "csv":
                print(self.path)
                df=spark.read.csv(path + "." + format)
                print(df)
                return df
        except Exception as e:
            return e
        

    def casting(self,df,casting_data):
        key_list = []
        for key in casting_data.keys():
            key_list.append(key)

        for column in key_list:
            if casting_data[column].split(",")[0] == "DecimalType":
                df = df.withColumn(column,df[column].cast(DecimalType(scale=int(casting_data[column].split(",")[1]))))
            elif casting_data[column] == "ArrayType-StringType":
                df = df.withColumn(column,concat_ws(",",col(column)))
        print("Data Casted")
        return df 
        
    def masking(self,df,masking_list):
        for column in masking_list:
            df = df.withColumn("masked_"+column,sha2(col(column),256))
        print("Data Masked")
        return df  
        
    def lookup_dataset(self,df,lookup_location,pii_cols,datasetName):
        datasetName1 = 'lookup'
        df_source = df.withColumn("begin_date",f.current_date())
        df_source = df_source.withColumn("update_date",f.lit("null"))
        
        pii_cols = [i for i in pii_cols if i in df.columns]
            
        columns_needed = []
        insert_dict = {}
        for col in pii_cols:
            if col in df.columns:
                columns_needed += [col,"masked_"+col]
        source_columns_used = columns_needed + ['begin_date','update_date']
        print(source_columns_used)

        df_source = df_source.select(*source_columns_used)

        try:
            targetTable = DeltaTable.forPath(spark,lookup_location+datasetName)
            delta_df = targetTable.toDF()
        except pyspark.sql.utils.AnalysisException:
            print('Table does not exist')
            df_source = df_source.withColumn("flag_active",f.lit("true"))
            df_source.write.format("delta").mode("overwrite").save(lookup_location)
            print('Table Created Sucessfully!')
            targetTable = DeltaTable.forPath(spark,lookup_location)
            delta_df = targetTable.toDF()
            delta_df.show(100)

        for i in columns_needed:
            insert_dict[i] = "updates."+i
            
        insert_dict['begin_date'] = f.current_date()
        insert_dict['flag_active'] = "True" 
        insert_dict['update_date'] = "null"
        
        print(insert_dict)
        
        _condition = datasetName1+".flag_active == true AND "+" OR ".join(["updates."+i+" <> "+ datasetName1+"."+i for i in [x for x in columns_needed if x.startswith("masked_")]])
        
        print(_condition)
        
        column = ",".join([datasetName1+"."+i for i in [x for x in pii_cols]]) 
        
        print(column)

        updatedColumnsToInsert = df_source.alias("updates").join(targetTable.toDF().alias(datasetName1), pii_cols).where(_condition) 
        
        print(updatedColumnsToInsert)

        stagedUpdates = (
          updatedColumnsToInsert.selectExpr('NULL as mergeKey',*[f"updates.{i}" for i in df_source.columns]).union(df_source.selectExpr("concat("+','.join([x for x in pii_cols])+") as mergeKey", "*")))

        targetTable.alias(datasetName1).merge(stagedUpdates.alias("updates"),"concat("+str(column)+") = mergeKey").whenMatchedUpdate(
            condition = _condition,
            set = {                  # Set current to false and endDate to source's effective date."flag_active" : "False",
            "update_date" : f.current_date()
          }
        ).whenNotMatchedInsert(
          values = insert_dict
        ).execute()

        for i in pii_cols:
            df = df.drop(i).withColumnRenamed("masked_"+i, i)

        return df
        
    def write_data(self, df,path,format,partition_columns):
        try:
            if format == "parquet":
                df.write.mode("append").partitionBy(partition_columns).parquet(path)
            elif format == "csv":
                df.write.csv(path + "." + format)
            return "successfully written"
        except Exception as e:
            return e
   
   

spark_config_path = "s3://aiswarya-landingzone/Configuration/spark_config_file.json"
datasetName = "Actives.parquet" 
dataset_path = "Actives.parquet" 

#creating object   
sparkjob=Configuration(spark_config_path,datasetName,dataset_path)

transform=Transformation()

rawzone_df = transform.reading_data(sparkjob.raw_source,sparkjob.raw_source_format)
            
casted_df = transform.casting(rawzone_df,sparkjob.transformation_columns)

df = transform.masking(casted_df, sparkjob.masking_columns)
        
# Creating a lookup dataset for the masking columns
        
df = transform.lookup_dataset(df,sparkjob.lookup_location,sparkjob.pii_cols,datasetName)
        
# Writing the data to Staging zone after transformation

transform.write_data(df,sparkjob.raw_destination,sparkjob.raw_destination_format,sparkjob.partition_columns)
          
# stop the spark session

spark.stop()