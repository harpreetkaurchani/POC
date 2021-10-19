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

