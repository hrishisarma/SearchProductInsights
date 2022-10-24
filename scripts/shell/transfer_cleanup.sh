date=$(date '+%Y-%m-%d')
echo $date
export AWS_ACCESS_KEY_ID=key
export AWS_SECRET_ACCESS_KEY=secret_key
mv /home/adobe/data/output/part* /home/adobe/data/output/$date'_SearchKeywordPerformance.tab'
aws s3 cp /home/adobe/data/output/$date'_SearchKeywordPerformance.tab' s3://adobedataexercise/output/$date/
# audit files
rm -r /home/adobe/data/
rm -r /home/adobe/scripts/python/