import sys
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import SparkSession

spark=SparkSession.builder.master("local").appName("BDA").getOrCreate()

#Loading data from CSV
#data=spark.read.csv(sys.argv[1],header=True)

data=spark.read.csv("data2.csv",header=True)



#borough validation

def boroughValidation(data):
	data=data.withColumn("Borough",when((col("Incident Zip")>10450) & (col("Incident Zip")<10475),"BRONX").otherwise(col("Borough")))
	data=data.withColumn("Borough",when((col("Incident Zip")>11200) & (col("Incident Zip")<11240),"BROOKLYN").otherwise(col("Borough")))
	data=data.withColumn("Borough",when((col("Incident Zip")>10000) & (col("Incident Zip")<10280),"MANHATTAN").otherwise(col("Borough")))
	data=data.withColumn("Borough",when((col("Incident Zip")>10300) & (col("Incident Zip")<10315),"STATEN ISLAND").otherwise(col("Borough")))
	data=data.withColumn("Borough",when((col("Incident Zip")>11350) & (col("Incident Zip")<11700),"QUEENS").otherwise(col("Borough")))
	data=data.withColumn("City", when((col("City").isNull()) | (col("City")=="N/A") | (col("City")=="NA"),"New_York").otherwise(col("City")))


def nullValues(x,y):
	y.append(data.select([count(when((col(x)=="NA")|(col(x)=="UNKNOWN") | (col(x)=="Unspecified") | (col(x)=="N/A") | (col(x)=="") | (col(x).isNull()) | (col(x)=="0 Unspecified"),x))]).take(1)[0][0])
	
# We create cData to store percentage of missing values in every column

cData=[]
cData2=[]
for i in data.columns:
	nullValues(i,cData)

length=data.count()
for i in range(0,len(cData)):
	cData[i]=(cData[i]/length)*100



# We drop columns with null values greater than the threshold value which is 50
#Headers used since columns are deleted so length(cData) becomes less which leads to index out of bounds

headers=data.columns
for i in range(0,len(cData)):
	if(cData[i]>50):
		data=data.drop(headers[i])   
		
		
#Write intermediate cleaned data to a CSV

#data.coalesce(1).write.option("header", "true").csv("intermediate.csv")



#Data after 1st level cleaning

for i in data.columns:
	nullValues(i,cData2)

for i in range(0,len(cData2)):
	cData2[i]=(cData2[i]/length)*100

cData2

	
# We create a SQL table

data.createOrReplaceTempView("temp")

# We remove all entries where the closed date, resolution action action date both are null and status is either closed or null

data=spark.sql("SELECT * FROM temp where `Closed Date` is not null and (status is not null or status!='Closed') and `Resolution Action Updated Date` is not null")

# We fill the missing closed date values based on the resolution action update date and vice versa

data=data.withColumn("Closed Date", when((col("Closed Date").isNull()) & (col("Status")=='Closed') &(col("Resolution Action Updated Date").isNotNull()),col("Resolution Action Updated Date")).otherwise(col("Closed Date")))
data=data.withColumn("Resolution Action Updated Date", when((col("Resolution Action Updated Date").isNull()) & (col("Closed Date").isNotNull()),col("Closed Date")).otherwise(col("Resolution Action Updated Date")))


#Remove all entries where closed date < created date and created date > current date

data.createOrReplaceTempView("temp")
data=spark.sql("select * from temp where to_date(`closed date`,'MM/dd/yyyy')>=to_date(`created date`,'MM/dd/yyyy')")


data=spark.sql("select * from temp where to_date(`created date`,'MM/dd/yyyy')<=now() or to_date(`closed date`,'MM/dd/yyyy')<=now()")


data.createOrReplaceTempView("temp")


#We drop the following columns since it has redundant data

data=data.drop('Location')
data=data.drop('Park Borough')



#RegEx is used to replace common values that has low frequences  and club them together

data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Ferry.*","Ferry Complaint"))
data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Highway.*","Highway Complaint"))
data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Noise.*","Noise Complaint"))
data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Taxi.*","Taxi Complaint"))

data=data.withColumn("Descriptor", when((col("Descriptor")=="N/A"),"No description").otherwise(col("Descriptor")))

data.createOrReplaceTempView("temp")








data=spark.sql("SELECT * FROM temp WHERE BOROUGH IN ('QUEENS','BROOKLYN','BRONX','MANHATTAN','STATEN ISLAND')")












data.coalesce(1).write.option("header", "true").csv("cleanData.csv")
















