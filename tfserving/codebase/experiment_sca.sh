#!/usr/bin/env bash
Pip="$1"
MODEL_REPLICAS="$2"
cwd=$(pwd)
PTH='../../tfserving'

cd $PTH
echo "========================================================="
echo "Scaling the number of parallel threads used for serving to '$MODEL_REPLICAS'..."
MAX_CPU="$(($MODEL_REPLICAS - 1))"
echo "Used CPU cores: 0-$MAX_CPU"
sed -i "s/cpuset: '[^']*'/cpuset: '0-$MAX_CPU'/g" docker-compose.yml
docker-compose up -d
sleep 10

cd $cwd
echo "Run on Pipeline $Pip"
mvn exec:java -Dexec.mainClass="ExperimentRunner" -Dexec.cleanupDaemonThreads=false -Dexec.args="$Pip $MODEL_REPLICAS"

docker-compose kill

sleep 10
~
~

