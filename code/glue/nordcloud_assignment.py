########################################
# Author     : Harpreet Kaur
# date       : 19 Oct 2021
# Description: It is a glue Python Shell Job which Read bitcoin csv file and create new file with previous year data
#               It also display the mean, max and min to std output along with plotting a line chart
# Trigger    : Currently this job is triggered by a lambda (lambda is based on S3 trigger). So as soon as file is dropped
#              in S3 lambda will be trigger which in turn click Glue Job
########################################
import boto3
import pandas as pd
from io import StringIO, BytesIO
from datetime import timedelta, datetime
import logging
import sys
import matplotlib.pyplot as plt
from nordcloud_assignment_properties import *
from awsglue.utils import getResolvedOptions

s3_client = boto3.client('s3')

def getLogger(name, level):
    '''
    :param name: logger name
    :param level: log level like INFO or DEBUG
    :return: logger obj
    '''
    logger = logging.getLogger(name)
    logger.setLevel(level)
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(level)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def convert_to_euro(data:pd.DataFrame, exchange_rate:float, column_name:str) -> pd.DataFrame:
    '''
    :param df: pandas dataframe with one column
    :param exchange_rate: float value needed to convert USD to Euro
    :return: pandas_dataframe with only one column converted to EURo from USD
    '''
    global logger
    try:
        return data.apply(lambda x: round(x*exchange_rate,2))
    except Exception as e:
        msg= f'Failed while converting values of column {column_name} from USD to EUR due to: {e}'
        raise Exception(msg)


def create_dataframe(file_path: str)-> pd.DataFrame:
    '''
    :param file_path: Source file path received from Lambda trigger as input_key
    :return: Pandas DataFrame created by reading the csv file
    '''
    global logger
    try:
        data= pd.read_csv(file_path)
        return data
    except Exception as e:
        msg = f'Failed while creating dataframe from csv file {file_path} due to: {e}'
        raise Exception(msg)


def convert_str_to_date(data:pd.DataFrame, column_name:str)-> pd.DataFrame:
    '''
    :param df: pandas dataframe with one colume that needs to be converted from str to date
    :param column_name: column name on which operation is applied
    :return: Pandas dataFrame with only Date field converted from str to Date type
    '''
    global logger
    try:
        return data.apply(lambda x: datetime.strptime(x, '%Y-%m-%d').date())
    except Exception as e:
        msg = f'Failed while converting data type of {column_name} to date type from string due to: {e}'
        raise Exception(msg)


def write_csv_to_s3(data:pd.DataFrame, columns:list, bucket:str, dest_path:str):
    '''
    :param df: final pandas dataframe with previous year data to be written to S3
    :param bucket: output bucket
    :param output_path: path in the output bucket to write the csv
    :param columns: columns to be written in the output csv file
    '''
    global logger
    try:
        with StringIO() as csv_buffer:
            data.to_csv(csv_buffer, columns=columns, index=False)
            _ = s3_client.put_object(
                Bucket=bucket, Key=dest_path+f"ingest_dt={datetime.now().strftime('%Y%m%d%H%M')}/output.csv",
                Body=csv_buffer.getvalue()
            )
    except Exception as e:
        msg=f'failed while writing csv to s3 bucket {bucket} at {dest_path} prefix due to: {e}'
        raise Exception(msg)


def save_plot_to_s3(plt:plt, bucket:str, image_path:str):
    '''
    :param plt: line plot created between date and volume
    :param bucket: dest bucket
    :param image_path: path in the output bucket to export the image of line plot
    '''
    global logger
    try:
        img_buffer = BytesIO()
        plt.savefig(img_buffer, format='png')
        img_buffer.seek(0)
        s3_client.put_object(Bucket=bucket, Body=img_buffer, ContentType='image/png',
                          Key=image_path+f"ingest_dt={datetime.now().strftime('%Y%m%d%H%M')}/plot.png")
    except Exception as e:
        msg=f'failed while writing plot as image to s3 bucket {bucket} at  {image_path} prefix due to: {e}'
        raise Exception(msg)


if __name__ == "__main__":
    global logger
    logger = getLogger('btc_filtering', log_level)
    args = getResolvedOptions(sys.argv, ['input_key'])

    try:
        logger.debug('Create dataframe using the file')
        raw_data = create_dataframe(args['input_key'])
        logger.debug('Convert the Date field from string to date for further operations')
        raw_data = raw_data.apply(lambda x: convert_str_to_date(x, x.name) if x.name == 'Date' else x)
        logger.debug('Filter the data for only last 365 days from the maximum data point')
        refined_data = raw_data[raw_data['Date'] >= (raw_data['Date'].max() - timedelta(days=365))]
        logger.debug('Convert Low and High fields to EUR from USD')
        final_data = refined_data.apply(lambda x: convert_to_euro(x, exchange_rate, x.name) if (x.name == 'Low' or x.name == 'High') else x)
        logger.debug('Rename Low and High fields to Low (EUR) and High (EUR) after conversion to Euros')
        final_data.rename(columns={'Low': 'Low (EUR)', 'High': 'High (EUR)'}, inplace=True)
        logger.info(f'write the results for previous year back to S3 at s3://{dest_bucket}/{dest_path}')
        write_csv_to_s3(final_data, columns_for_output_csv, dest_bucket, dest_path)
        logger.debug('Prepare data to print for std output by calculating max, min and mean. '
                     'Also convert Volume to Billion')
        data = {'min': [final_data['Low (EUR)'].min(), final_data['High (EUR)'].min(), final_data['Volume'].min()/1000000000],
                'max': [final_data['Low (EUR)'].max(), final_data['High (EUR)'].max(), final_data['Volume'].max()/1000000000],
                'mean': [final_data['Low (EUR)'].mean(), final_data['High (EUR)'].mean(), final_data['Volume'].mean()/1000000000]}
        logger.debug('Set precision for float numbers in Pandas')
        pd.set_option('display.float_format', lambda x: '%.2f' % x)
        logger.debug('Create dataframe for std output with index')
        std_data = pd.DataFrame(data, index=['Low (EUR)', 'High (EUR)', 'Volume (Billion)'], dtype=float)
        print(std_data)
        try:
            logger.info('Plot a line chart for Low (EUR) and high (EUR) for previous year data')
            plt.plot(final_data['Date'], final_data['Low (EUR)'], linestyle="-", label='Low')
            plt.plot(final_data['Date'], final_data['High (EUR)'], linestyle="-", label='High')
            plt.title('Low vs High')
            plt.xlabel('Date')
            plt.ylabel('Volume')
            plt.legend()
            #plt.show()
            save_plot_to_s3(plt, dest_bucket, image_path)
        except Exception as e:
            msg = f'failed while plotting the line chart due to: {e}'
            raise Exception(msg)
    except Exception as e:
        logger.error(e)
        sys.exit(1)
