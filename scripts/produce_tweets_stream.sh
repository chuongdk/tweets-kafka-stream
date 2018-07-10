export DATA_DIR="data"
while :
do
	for path in $DATA_DIR/*
	do
		echo $path
		kafka-console-producer --broker-list 0.0.0.0:9092 --topic tweets < $path
		sleep 1
	done
done
