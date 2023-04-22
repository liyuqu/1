#!/usr/bin/env bash
Pip="$1"
cwd=$(pwd)
PTH='../../tfserving'
cd $PTH
sed -i "s/cpuset: '[^']*'/cpuset: '0-0'/g" docker-compose.yml
docker-compose up -d
sleep 10

cd $cwd
echo "Run on Pipeline $Pip"
mvn exec:java -Dexec.mainClass="ExperimentRunner" -Dexec.cleanupDaemonThreads=false -Dexec.args="$Pip"

docker-compose kill

sleep 10
