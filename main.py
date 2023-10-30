file_location = "/FileStore/tables/MOCK_DATA__1_.csv"
from pyspark.sql import SparkSession
file_type = "csv"
 
spark =SparkSession.builder.appName('data').getOrCreate()
data=spark.read.csv(file_location,inferSchema=True,header=True)

from pyspark.ml.feature import VectorAssembler
assemlber=VectorAssembler(inputCols=['Quarter','No_of_employee','Marketing_spend','R&D_spend','Total_Customers'],outputCol='independent_feature')
data_r=assemlber.transform(data)

from pyspark.ml.regression import LinearRegression
train_data,test_data=data_r.randomSplit([0.75,0.25])
path="dbfs:/FileStore/tables/saved_model/"
regression=LinearRegression(featuresCol='independent_feature',labelCol='Revenue')
regression1=regression.fit(train_data)
regression1.write().overwrite().save(path)

from pyspark.ml.regression import LinearRegressionModel
loaded_model=LinearRegressionModel.read().load(path)
predictions=loaded_model.evaluate(data_r)
predictions.predictions
sfOptions = {
    "sfURL" : "********",
    "sfUser":"*****",
    "sfPassword":"******",
    "sfDatabase" : "********",
    "sfWarehouse" : "COMPUTE_WH",
    "sfRole" : "ACCOUNTADMIN",
    "sfSchema" : "PUBLIC"}

processed_data=predictions.predictions.select("Quarter","No_of_employee","Marketing_spend","R&D_spend","Total_Customers","Revenue","prediction")
processed_data.write.format("snowflake").options(**sfOptions).option("dbtable", "DATA").mode("append").save()
