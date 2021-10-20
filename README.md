# POC
### This Project Does Following
 1. Lambda under Code is based on S3 trigger.
 2. Lambda take the S3 bucket and file prefix information from event and construct the input_path for csv
 3. Lambda Trigger the Glue Job and pass the input_key as argument to the job
 4. Glue Job read the file from path passed by Lambda
 5. It filters out previous year data and store it in S3 again
 6. It also show the mean, max and min on std output
 7. It plot a graph for Low and High Volume and store it in S3 as a png image

### Dependencies needed by Glue Job to write csv back to S3 and to plot the graph are kept under dependencies folder.
   These dependecies needs to be added while creating the Glue Job
   
### Steps To execute the code
  1. Update value for dest_bucket in code/glue/nordcloud_assignment_properties.py file.
  2. Upload code folder from repo to one S3 bucket.
  3. Now create Glue Job of type Python Shell(Python3 version) using code/glue/nordcloud_assignment.py
  4. As we are using nordcloud_assignment_properties.py file so add s3 path of this file as the "Referenced files path" while creating the glue Job.
  5. To draw the line chart we are using matplotlib.pyplot module which is not supported by Glue Internally. So we have pre-downloaded .whl file and kept under code/dependencies folder. Also we need .whl file for s3fs as we want to write back csv file to S3. Now add s3 location of both of these .whl files as comma separated values under "Python library path". Eveytime we run Glue job these libraries will be installed and kept ready for job execution.
  6. Make sure to provide a IAM role with access to S3 and Cloudwatch.
  7. Once Glue Job is created we need a lambda to trigger this Job.
  8. So Now Create lambda using the code provided under code/lambda/norcloud_trigger_lambda.py
  9. Add LOG_LEVEL , Glue_Job_Name, AWS_REGION_NAME environment variables to the lambda.
  10. Make sure the IAM role used for this lambda has access to trigger Glue Job.
  11. Also make sure to update the Runtime of lambda to 15 minutes as by default its 3 sec.
  12. Now we have our lambda ready which is internally calling the glue Job.
  13. To trigger this lambda as soon as input csv file arrives in the S3 bucket , we need to add a S3 trigger to the lambda.
  14. So add a S3 trigger using S3 bucket where input csv file will be loaded. Add the prefix(must be same to the one used while creating the trigger on lambda) along with suffix(.csv) to avoid unnecessory trigger of lambda for any other files coming to the same bucket.
  15. Now we are ready with everything So just go ahead and drop the csv file to the input S3 bucket at the given S3 location.
  16. As soon as file is dropped lambda will be triggered and lambda will trigger glue job internally.
  17. You will be able to see the output csv under dest_path and image of plot under image_path , Once Glue job is completed successfully.

