%pyspark
## Assumptions
# 1) Here raw_data is coming to s3 with prefix da
# 2) Here client bucket_name is test-adb

import boto3
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import datetime

date_str = datetime.datetime.now().strftime("%Y-%m-%d")
bucket_name = 'test-adb'
input_key_prefix = 'raw_data/da' 
output_key_prefix = f'output/{date_str}/par' 

class KeywordPerformance:
    
    def __init__(self, bucket_name, input_key_prefix, output_key_prefix):
        # initialize inputs
        self.bucket_name = bucket_name
        self.input_key_prefix = input_key_prefix
        self.output_key_prefix = output_key_prefix
        self.date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        self.s3 = boto3.client('s3')
        self.df = None
        
    def get_recent_file(self, prefix):
        # get recent file from s3 bucket
        objects = self.s3.list_objects(Bucket=self.bucket_name, Prefix=prefix)
        recent_object = sorted(objects['Contents'], key=lambda obj: obj['LastModified'])[-1]
        return recent_object['Key']

    def read_data(self):
        # reading tsv file from s3
        df = spark.read.format("csv").option("delimiter", "\t").option("header", True).load("s3://{}/{}".format(self.bucket_name, self.get_recent_file(self.input_key_prefix)))
        df.cache()
        df.registerTempTable('sample')
        self.df = df
    
    def read_eng(self):
        self.read_data()
        # Getting search engine
        df_eng = self.df.select("*", F.row_number().over(Window.partitionBy("ip").orderBy("hit_time_gmt",F.desc("event_list"))).alias("rowNum")).filter("rowNum=1").select("ip",F.split("referrer",'/')[2].alias("engine"))
        return df_eng
    
    def read_keyword(self):
        self.read_data()
        # getting key words from data
        df_key = self.df.filter("event_list=1").select("*", F.row_number().over(Window.partitionBy("ip").orderBy("hit_time_gmt")).alias("rowNum")).select("ip",F.explode(F.split("product_list",'\\,')).alias("info"))
        df_key = df_key.select("ip",F.split("info",'\\;')[1].alias("key_word"),F.split("info",'\\;')[3].alias("Revenue"))
        return df_key
    
    def final(self):
        # Getting revenue and ordering data based on revenue in desc order
        final = self.read_eng().join(self.read_keyword(),["ip"],"inner").select(F.col("engine").alias("Engine_Domain"),F.split("key_word","-")[0].alias("Keyword"),"Revenue").orderBy(F.desc("Revenue"))
        final.repartition(1).write.format("csv").mode("overwrite").option("delimiter", "\t").option("header", True).save(f"s3://{self.bucket_name}/output/{self.date_str}/")
        final.show()
    
    def rename_output_file(self):
        # Renaming output file based on given conditions
        src_bucket = self.bucket_name
        src_key = self.get_recent_file(self.output_key_prefix)
        dst_bucket = self.bucket_name
        dst_key = f"output/{self.date_str}/{self.date_str}_SearchKeywordPerformance.tsv"
        s3.copy_object(Bucket=dst_bucket, CopySource={'Bucket': src_bucket, 'Key': src_key}, Key=dst_key)
        s3.delete_object(Bucket=src_bucket, Key=src_key)

    def run(self):
        self.final()
        self.rename_output_file()


kp = KeywordPerformance(bucket_name, input_key_prefix, output_key_prefix)
kp.run()