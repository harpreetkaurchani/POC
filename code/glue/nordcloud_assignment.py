########################################
# Author : Harpreet Kaur
# date: 19 Oct 2021
# Description: Read bitcoin csv file and create new file with previous year data
#               It also display the mean, max and min to std output along with plotting a line chart
########################################
import boto3
import pandas
import pandas as pd
from io import StringIO, BytesIO
from datetime import timedelta, datetime
import logging
import sys
import matplotlib.pyplot as plt
from nordcloud_assignment_properties import *
global logger


def convert_to_euro(df:pandas.DataFrame, exchange_rate:float) -> pandas.DataFrame:
    '''
    :param df:
    :param exchange_rate:
    :return:
    '''
    try:
        return df.apply(lambda x: round(x*exchange_rate,2))
    except Exception as e:
        msg= f'Failed while converting USD to EUR due to: {e}'
        raise Exception(msg)


def create_dataframe(file_path: str)-> pandas.DataFrame:
    '''
    :param file_path:
    :return:
    '''
    try:
        df= pd.read_csv(file_path)
        return df
    except Exception as e:
        msg = f'Failed while creating csv file due to: {e}'
        raise Exception(msg)


def convert_str_to_date(df:pandas.DataFrame, column_name:str)-> pandas.DataFrame:
    '''
    :param df:
    :param column_name:
    :return:
    '''
    try:
        return df.apply(lambda x: datetime.strptime(x, '%Y-%m-%d').date())
    except Exception as e:
        msg = f'Failed while converting {column_name} to date type from string due to: {e}'
        raise Exception(msg)


def write_csv_to_s3(df:pandas.DataFrame, columns:list, bucket:str, output_path:str):
    '''
    :param df:
    :param bucket:
    :param output_path:
    :param columns:
    :return:
    '''
    try:
        s3_client = boto3.client('s3')
        with StringIO() as csv_buffer:
            df.to_csv(csv_buffer, columns=columns, index=False)
            _ = s3_client.put_object(
                Bucket=bucket, Key=output_path, Body=csv_buffer.getvalue()
            )
    except Exception as e:
        msg=f'failed while writting csv to s3 due to: {e}'
        raise Exception(msg)


def save_plot_to_s3(plt, bucket, image_path):
    '''
    :param plt:
    :param bucket:
    :param image_path:
    :return:
    '''
    try:
        img_buffer = BytesIO()
        plt.savefig(img_buffer, format='png')
        img_buffer.seek(0)
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket)
        bucket.put_object(Body=img_buffer, ContentType='image/png', Key=image_path)
    except Exception as e:
        msg=f'failed while writing plot as image to s3 due to: {e}'
        raise Exception(msg)


if __name__ == "__main__":
    global logger
    logger = logging.getLogger('btc_filtering')

    try:
        logger.debug('Create dataframe using the file')
        df = create_dataframe(source_path)
        logger.debug('Convert the Date field from string to date for further operations')
        df = df.apply(lambda x: convert_str_to_date(x, x.name) if x.name == 'Date' else x)
        logger.debug('Filter the data for only last 365 days from the maximum data point')
        df1 = df[df['Date'] >= (df['Date'].max() - timedelta(days=365))]
        logger.debug('Convert Low and High fields to EUR from USD')
        df2 = df1.apply(lambda x: convert_to_euro(x, exchange_rate) if (x.name == 'Low' or x.name == 'High') else x)
        logger.debug('Rename Low and High fields to Low (EUR) and High (EUR) after conversion to Euros')
        df2.rename(columns={'Low': 'Low (EUR)', 'High': 'High (EUR)'}, inplace=True)
        logger.info(f'write the results for previous year back to S3 at s3://{dest_bucket}/{dest_path}')
        write_csv_to_s3(df2, columns_for_output_csv, dest_bucket, dest_path)
        #df2.to_csv('output.csv', columns=['Date', 'Low (EUR)', 'High (EUR)'], index=False)
        logger.debug('Prepare data to print for std output by calculating max, min and mean. '
                     'Also convert Volume to Billion')
        data = {'min': [df2['Low (EUR)'].min(), df2['High (EUR)'].min(), df2['Volume'].min()/1000000000],
                'max': [df2['Low (EUR)'].max(), df2['High (EUR)'].max(), df2['Volume'].max()/1000000000],
                'mean': [df2['Low (EUR)'].mean(), df2['High (EUR)'].mean(), df2['Volume'].mean()/1000000000]}
        logger.debug('Set precision for float numbers in Pandas')
        pd.set_option('display.float_format', lambda x: '%.2f' % x)
        logger.debug('Create dataframe for std output with index')
        std_df = pd.DataFrame(data, index=['Low (EUR)', 'High (EUR)', 'Volume (Billion)'], dtype=float)
        print(std_df)
        try:
            logger.info('Plot a line chart for Low (EUR) and high (EUR) for previous year data')
            plt.plot(df2['Low (EUR)'], df2['High (EUR)'])
            plt.title('Low vs High')
            plt.xlabel('Low')
            plt.ylabel('High')
            plt.show()
            save_plot_to_s3(plt, dest_bucket, image_path)
        except Exception as e:
            msg = f'failed while plotting the line chart due to: {e}'
            raise Exception(msg)
    except Exception as e:
        logger.error(e)
        sys.exit(1)
