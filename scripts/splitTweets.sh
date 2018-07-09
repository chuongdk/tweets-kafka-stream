mkdir data
cd data
echo "Split data to "
echo $(pwd)
split -l 100 --numeric-suffixes --additional-suffix .json ../tweets-v2.json stream_tweets
