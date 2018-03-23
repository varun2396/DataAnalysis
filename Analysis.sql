data=spark.read.csv("cleanData.csv",header=True)
data.createOrReplaceTempView("temp")


1,6.
--spark.sql("select distinct `complaint type`,count(`complaint type`) as cnt from temp  group by `complaint type`,year(TO_DATE(CAST(UNIX_TIMESTAMP(`created date`, 'MM/dd/yyyy') AS TIMESTAMP))) order by cnt desc").show() 

To remove invalid date left in code.....
spark.sql("select distinct `complaint type`,count(`complaint type`) as cnt from temp where to_date(`closed date`,'MM/dd/yyyy')>=to_date(`created date`,'MM/dd/yyyy')  group by `complaint type`,year(TO_DATE(CAST(UNIX_TIMESTAMP(`created date`, 'MM/dd/yyyy') AS TIMESTAMP))) order by cnt desc").show()   

Yearly: Just replace 2017 with the year you want
spark.sql("select distinct `complaint type`,count(`complaint type`) as cnt from temp where upper(`complaint type`) like 'NOISE%' and `created date` like '%2017%' and to_date(`closed date`,'MM/dd/yyyy')>=to_date(`created date`,'MM/dd/yyyy') group by `complaint type`,year(TO_DATE(CAST(UNIX_TIMESTAMP(`created date`, 'MM/dd/yyyy') AS TIMESTAMP))) order by cnt desc").show()

Monthly:
spark.sql("select distinct `complaint type`,count(`complaint type`) as cnt from temp where upper(`complaint type`) like 'NOISE%' and `created date` like '%01%2017%' and to_date(`closed date`,'MM/dd/yyyy')>=to_date(`created date`,'MM/dd/yyyy') group by `complaint type`,year(TO_DATE(CAST(UNIX_TIMESTAMP(`created date`, 'MM/dd/yyyy') AS TIMESTAMP))) order by cnt desc").show()


2.
spark.sql("select distinct `location type`,count(`location type`) as cnt from temp where to_date(`closed date`,'MM/dd/yyyy')>=to_date(`created date`,'MM/dd/yyyy') group by `location type`,year(TO_DATE(CAST(UNIX_TIMESTAMP(`created date`, 'MM/dd/yyyy') AS TIMESTAMP))) order by cnt desc").show() 

spark.sql("select distinct `location type`,count(`location type`) as cnt from temp where to_date(`closed date`,'MM/dd/yyyy')>=to_date(`created date`,'MM/dd/yyyy') and upper(`location type`) like 'RESIDENTIAL%' and `created date` like '%2017%' group by `location type`,year(TO_DATE(CAST(UNIX_TIMESTAMP(`created date`, 'MM/dd/yyyy') AS TIMESTAMP))) order by cnt desc").show()


3.
spark.sql("select distinct `borough`,count(`borough`) as cnt from temp where to_date(`closed date`,'MM/dd/yyyy')>=to_date(`created date`,'MM/dd/yyyy') and `created date` like '%2017%' group by `borough`,year(TO_DATE(CAST(UNIX_TIMESTAMP(`created date`, 'MM/dd/yyyy') AS TIMESTAMP))) order by cnt desc").show()

4. where complaint type
spark.sql("select datediff(to_date(`closed date`,'MM/dd/yyyy'), to_date(`created date`,'MM/dd/yyyy')) AS d,`created date`,`closed date`,`complaint type` from temp where to_date(`closed date`,'MM/dd/yyyy')>=to_date(`created date`,'MM/dd/yyyy') order by d desc").show()

5. Replace boruogh values
spark.sql("select datediff(to_date(`closed date`,'MM/dd/yyyy'), to_date(`created date`,'MM/dd/yyyy')) AS d,`created date`,`closed date`,`complaint type` from temp where borough='QUEENS' and to_date(`closed date`,'MM/dd/yyyy')>=to_date(`created date`,'MM/dd/yyyy') order by d desc").show()