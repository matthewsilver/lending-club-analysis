import time
import pyspark.sql.functions as F
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET_KEY)

class LoanData():
    def __init__(self, config):
        self.bucket = config['bucket']
        self.raw_folder = config['raw_folder']
        self.processed_folder = config['processed_folder']
        self.loans_raw = None
        self.loans_processed = None
    def readData(self, bucket, folder):
        self.loans_raw = spark.read.option('header', 'true').csv('s3://{}/{}'.format(bucket, folder))
        self.loans_raw.cache()
    def writeData(self, df, bucket, folder, partition_key):
        df.write.mode('append').partitionBy(partition_key).parquet("s3n://{}/{}".format(bucket, folder))
    def processLoans(self):
        self.loans_processed = sqlContext.sql('''
        with loan_statuses_transformed as
          (
            select
            "{}" as collection_timestamp,
              (case
                when loan_status in ('Fully Paid', 'Current') then 'good_standing'
                when loan_status in ('Charged Off') or loan_status like 'Late%' then 'bad_standing'
                else 'unknown'
                end
              ) as loan_status_simplified,
            loan_status,
            int(loan_amnt) as loan_amnt,
            round(loan_amnt, -4) as loan_amnt_rounded,
            title as loan_title,
            int_rate,
            grade,
            emp_title,
            emp_length,
            float(dti) as dti,
            home_ownership,
            int(annual_inc) as annual_inc,
            pow(10, round(log10(annual_inc), 0)) as annual_inc_rounded,
            verification_status,
            purpose,
            delinq_2yrs,
            earliest_cr_line,
            inq_last_6mths,
            mths_since_last_delinq,
            open_acc,
            pub_rec
            from loan_data 
          )
        select * from loan_statuses_transformed
        '''.format(time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())))
        self.loans_processed.cache()