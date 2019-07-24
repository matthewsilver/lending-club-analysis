import time
import pyspark.sql.functions as F

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET_KEY)

class LoanData():
    def __init__(self, config):
        """
        Initialize Loan Dataset object.
        Input: config, a dictionary with the following keys:
            > bucket (AWS S3 bucket raw loan data is read from)
            > raw_folder (folder inside AWS S3 bucket where raw loan data is read)
            > processed_folder (folder inside AWS S3 bucket where processed loan data will be written)
        """
        self.bucket = config['bucket']
        self.raw_folder = config['raw_folder']
        self.processed_folder = config['processed_folder']
        self.loans_raw = None
        self.loans_processed = None
        self.loan_status_distribution = None
        self.annual_inc_summary = None
    def readData(self, bucket, folder):
        """
        Read CSV loan data from an AWS S3 bucket and folder
        Input: AWS S3 bucket and folder to read from
        """
        self.loans_raw = spark.read.option('header', 'true').csv('s3://{}/{}'.format(bucket, folder))
        self.loans_raw.cache()
    def writeData(self, df, bucket, folder, partition_key):
        """
        Write loan data in parquet format to an AWS S3 bucket and folder
        Input:
            > df: dataframe to be written out
            > bucket: AWS S3 bucket to write to
            > folder: AWS S3 folder to write to
            > partition_key: key to partition on when writing out parquet
        """
        df.write.mode('append').partitionBy(partition_key).parquet("s3n://{}/{}".format(bucket, folder))
    def processLoans(self):
        """
        Perform preprocessing on raw loan data to facilitate exploratory data analysis and ETL pipelining
        Input: none, but make sure readData() has already been run
        """
        self.loans_raw.createOrReplaceTempView('loan_data')

        # Perform the following transformations on the data:
        # Simplify loan statuses to good, bad, or unknown standing
        # Round loan amount and log of annual income to allow for analyses with bucketing
        # Cast annual income field to int type
        # Add a collection_timestamp field indicating when the data was processed to allow for new batches of data
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
            round(int(loan_amnt), -4) as loan_amnt_rounded,
            pow(10, round(log10(int(annual_inc)), 0)) as annual_inc_rounded,
            *
            from loan_data
          )
        select * from loan_statuses_transformed
        '''.format(time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())))
        self.loans_processed = self.loans_processed.withColumn('annual_inc', F.col('annual_inc').cast('int'))
        self.loans_processed = self.loans_processed.withColumn('dti', F.col('dti').cast('float'))
        self.loans_processed.cache()

def main():
    config = {
      'bucket': 'lending-club-warehouse',
      'raw_folder': 'raw_loan_data',
      'processed_folder': 'processed'
    }

    l = LoanData(config)
    l.readData(l.bucket, l.raw_folder)
    l.processLoans()
    l.writeData(l.loans_processed, l.bucket, l.processed_folder, 'collection_timestamp')

if __name__ == '__main__':
    main()