from pyspark.sql import SparkSession

S3_DATA_INPUT_PATH="s3://project-s3-athena/source-file/12_million_plus_company_data.csv"
S3_DATA_OUTPUT_PATH_FILTERED="s3://project-s3-athena/data-output/data-output/filtered"

def main():
    spark = SparkSession.builder.appName('projectProDemo').getOrCreate()
    df = spark.read.json(S3_DATA_INPUT_PATH)
    print(f'The total number of records in the source data set is {df.count()}')
    filtered_df = df.filter((df.country == 'vietnam'))
    print(f'The total number of records in the filtered data set is {filtered_df.count()}')
    filtered_df.show(10)
    filtered_df.printSchema()
    filtered_df.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_FILTERED)
    print('The filtered output is uploaded successfully')

if __name__ == '__main__':
    main()
