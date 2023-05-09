# Import
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com') \
                    .getOrCreate()
                    
# Create DataFrame
df = spark.read \
          .option("header",True) \
          .csv("./data/simple-zipcodes.csv")
df.printSchema()
df.show()
print('Create SQL table')
# Create SQL table
spark.read \
          .option("header",True) \
          .csv("./data/simple-zipcodes.csv") \
          .createOrReplaceTempView("Zipcodes")
          
print('DF Demo select query ')
# Select query
df.select("country","city","zipcode","state") \
     .show(5)

print('SparkSQL Demo select query ')
spark.sql("SELECT  country, city, zipcode, state FROM ZIPCODES") \
     .show(5)
     
print('DF Demo Where clause')
# where
df.select("country","city","zipcode","state") \
  .where("state == 'AZ'") \
  .show(5)

print('SparkSQL Demo Where clause')
spark.sql(""" SELECT  country, city, zipcode, state FROM ZIPCODES 
          WHERE state = 'AZ' """) \
     .show(5)
     
print('DF Demo Sorting')
# sorting
df.select("country","city","zipcode","state") \
  .where("state in ('PR','AZ','FL')") \
  .orderBy("state") \
  .show(10)
  
print('SparkSQL Demo Sorting')
spark.sql(""" SELECT  country, city, zipcode, state FROM ZIPCODES 
          WHERE state in ('PR','AZ','FL') order by state """) \
     .show(10)

print('DF Demo Grouping')
# grouping
df.groupBy("state").count() \
  .show()

print('SparkSQL Demo Grouping')
spark.sql(""" SELECT state, count(*) as count FROM ZIPCODES 
          GROUP BY state""") \
     .show()
df.write.format("json").save("./data/json_data/aa.json")     
