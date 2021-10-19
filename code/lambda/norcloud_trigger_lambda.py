#############################################################
# Author        : Harpreet Kumar
# Date          : 19-10-2021
# Description   : This Lambda will be triggered for any new bit coin file in S3 bucket
#                  and will triggerGlue job for further processing
#############################################################

import json
import boto3
import os
import logging
import sys
from datetime import datetime

glue_client = boto3.client('glue', region_name=os.environ['AWS_REGION_NAME'])


def getLogger(name, level):
    """
    Descritpion: Create logger object
    Args:
        name: Name of the logger Object
        level: Logging Level
    Return: Obj of logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(level)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def lambda_handler(event, context):
    try:
        logger = getLogger('TriggerGlueJob', os.environ['LOG_LEVEL'])
        logger.info("event : {}".format(event))
        key = event.get('Records')[0].get('s3').get('object').get('key').replace('%3D', '=')
        bucket = event.get('Records')[0].get('s3').get('bucket').get('name')
        logger.info(f"Lambda triggered for object {key} in bucket {bucket}")
        inputParameters = {"--input_key": 's3://{}/{}'.format(bucket, key)}
        logger.info(f"call Glue job {os.environ['Glue_Job_Name']} with input {inputParameters}")
        try:
            response = glue_client.start_job_run(
                JobName= os.environ['Glue_Job_Name'],
                Arguments= inputParameters
            )
        except Exception as e:
            raise Exception(f"Failed to start Glue job due to: {e}")

        return {
            'body': json.dumps('Success')
        }
    except Exception as e:
        logger.error("Lambda execution failed: {}".format(e))
