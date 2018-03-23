Run the following commands

1) Download data to be cleaned 
wget https://data.cityofnewyork.us/api/views/wpe2-h2i5/rows.csv?accessType=DOWNLOAD
2) Rename data
mv rows.csv\?accessType\=DOWNLOAD data.csv
3) Upload data into the cluster
hfs -put data.csv
4) Initialize modules fo pyspark2 since our code is for pyspark2(NYU Dumbo)
module load java/1.8.0_72
module load spark/2.2.0
module load python/gnu/3.4.4
export PYSPARK2_PYTHON=/share/apps/python/3.4.4/bin/python3
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python3

5) Execute the code
pyspark2-submit Cleaning.py data.csv 

6) Retrieve the output CSV files
hfs -getmerge intermediate.csv intermediate.csv
hfs -getmerge cleanData.csv clean.csv

Note:
To re-execute, delete the csv files generated
hfs -rm -r intermediate.csv
hfs -rm -r cleanData.csv

