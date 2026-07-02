from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

spark=SparkSession.builder.master("local").appName("demo").getOrCreate()
sc=spark.sparkContext

Category_data = [
    ("SA01", "New user"),
    ("SA02", "Amend user"),
    ("SA03", "Delete user"),
    ("SA04", "New profile"),
    ("SA05", "Amend profile"),
    ("SA06", "Delete profile"),
    ("LL01", "Successful login"),
    ("LL02", "Successful logoff"),
    ("LL03", "Failed authentication"),
    ("LL04", "Password change success"),
    ("LL05", "Password change failure")
]
Category_schema=StructType([StructField("Category_type",StringType(),True),StructField("Category_description",StringType(),True)])
Category_df=spark.createDataFrame(Category_data,Category_schema)
Category_df.show()


df1=spark.read.json("gs://adarshmd28/FR_logs_all.json")
df1.printSchema()
df2=df1.select(col("timestamp").alias("timestamp"),
               col("client.ip").alias("client_ip"),
               col("client.port").alias("client_port"),
               col("server.ip").alias("server_ip"),
               col("server.port").alias("server_port"),
               col("request.operation").alias("request_operation"),
               col("request.dn").alias("request_dn"),
               col("request.filter").alias("request_filter"),
               col("response.status").alias("response_status"),
               col("response.statuscode").alias("response_statuscode"),
               col("response.reason").alias("response_reason")
              )
df2.show()
df3=df2.filter(col("request_operation")!="TLS").filter(col("request_operation")!="DISCONNECT").filter(col("request_operation")!="SEARCH")
df3.printSchema()
userdn_list=["ou=people,ou=global,dc=dbgroup,dc=com","ou=expeople,ou=global,dc=dbgroup,dc=com"]
df4=df3.withColumn("Category",when((col("request_operation")=="BIND") & (col("response_status")=="SUCCESSFUL"),lit("LL01")).
                  when((col("request_operation")=="BIND") & (col("response_status")=="FAILED"),lit("LL03")).
                  when(col("request_operation")=="UNBIND",lit("LL02")).
                  when((col("request_operation")=="MODIFY") & (col("response_status")=="SUCCESSFUL") & ((col("request_dn").contains("ou=people,ou=global,dc=dbgroup,dc=com")) | (col("request_dn").contains("ou=expeople,ou=global,dc=dbgroup,dc=com"))),lit("SA02")).
                  when((col("request_operation")=="ADD") & (col("response_status")=="SUCCESSFUL") & ((col("request_dn").contains("ou=people,ou=global,dc=dbgroup,dc=com")) | (col("request_dn").contains("ou=expeople,ou=global,dc=dbgroup,dc=com"))),lit("SA01")).
                  when((col("request_operation")=="DELETE") & (col("response_status")=="SUCCESSFUL") & ((col("request_dn").contains("ou=people,ou=global,dc=dbgroup,dc=com")) | (col("request_dn").contains("ou=expeople,ou=global,dc=dbgroup,dc=com"))),lit("SA03")).
                  when((col("request_operation")=="MODIFY") & (col("response_status")=="SUCCESSFUL") & (~((col("request_dn").contains("ou=people,ou=global,dc=dbgroup,dc=com")) | (col("request_dn").contains("ou=expeople,ou=global,dc=dbgroup,dc=com")))),lit("SA05")).
                  when((col("request_operation")=="ADD") & (col("response_status")=="SUCCESSFUL") & (~((col("request_dn").contains("ou=people,ou=global,dc=dbgroup,dc=com")) | (col("request_dn").contains("ou=expeople,ou=global,dc=dbgroup,dc=com")))),lit("SA04")).
                  when((col("request_operation")=="DELETE") & (col("response_status")=="SUCCESSFUL") & (~((col("request_dn").contains("ou=people,ou=global,dc=dbgroup,dc=com")) | (col("request_dn").contains("ou=expeople,ou=global,dc=dbgroup,dc=com")))),lit("SA06")))


df4.show()
folder_name=datetime.now().strftime("%Y%m%d%H%M%S")
df4.coalesce(1)
df4.write.mode("overwrite").option("header",True).csv("gs://dataproc_output_file/"+folder_name+"/"+"processed")

summary_df=df4.join(Category_df,df4.Category==Category_df.Category_type,"inner").drop(col("Category_df.Category_type"))
df5=summary_df.groupBy("Category","Category_description").agg(count("*").alias("counts")).orderBy("Category")
df5.coalesce(1)
df5.write.mode("overwrite").option("header",True).csv("gs://dataproc_output_file/"+folder_name+"/"+"summary")
print("done")
