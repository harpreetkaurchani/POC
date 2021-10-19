exchange_rate = 0.87
source_bucket='testishan'
dest_bucket='testishan'
source_path = f's3://{source_bucket}/nordcloudpoc/btc_data/input/btc.csv'
dest_path = 'nordcloudpoc/btc_data/output/btc.csv'
image_path = 'nordcloudpoc/btc_data/plot/plot.png'
columns_for_output_csv = ['Date', 'Low (EUR)', 'High (EUR)']