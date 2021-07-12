import pandas
import json
import logging
import time
import boto3
from botocore.exceptions import ClientError


def main():
    """Practice 01 Kinesis Firehose to S3 methods """

    # Assign these values before running the program
    # TODO: for the next version the name of the file should be a parameter
    delivery_stream_name = 'firehose_to_s3_stream_from_py'
    data_file = 'data/movies-netflix.zip'
    firehose_client = boto3.client('firehose')

    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s: %(asctime)s: %(message)s')

    # TODO: Should validate If the specified IAM role does not exist, it should be created
    # TODO: Should validate If Firehose doesn't exist, it should be created

    # Read the file using pandas
    record = {}
    df = pandas.read_csv(data_file, compression='zip', sep=',',
                         header=None, names=['MovieID', 'YearOfRelease', 'Title'])
    df['YearOfRelease'] = df['YearOfRelease'].fillna(0.0).astype(int)

    # Mark the start in log
    logging.info('\nPutting records into the Firehose one at a time')

    # Start to iterate through the dataset to get one record
    # TODO: I have to another version to test the batch mode
    for index, row in df.iterrows():
        record = {
            'MovieID': row['MovieID'],
            'YearOfRelease': row['YearOfRelease'],
            'Title':  row['Title']
        }
        try:
            # Put the record into the Firehose stream
            response = firehose_client.put_record(
                DeliveryStreamName=delivery_stream_name,
                Record={
                    'Data': json.dumps(record)
                }
            )
            logging.info(
                '\nData Record sent to Kinesis Data Firehose : \n' + str(Record))
            # TODO: ask Luis Why I have to wait 500ms between each message
            time.sleep(.5)
        except ClientError as e:
            logging.error(e)
            exit(1)

    logging.info('\nAll data has been sent to Firehose stream!\n')


if __name__ == '__main__':
    main()
