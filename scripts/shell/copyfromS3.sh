mkdir -p /home/adobe/data/input/
mkdir -p /home/adobe/scripts/python/
export AWS_ACCESS_KEY_ID=key
export AWS_SECRET_ACCESS_KEY=secret_key
aws s3 cp s3://adobedataexercise/input/data2.tsv /home/adobe/data/input/
aws s3 cp s3://adobedataexercise/scripts/revenue_product/ /home/adobe/scripts/python/ --recursive
