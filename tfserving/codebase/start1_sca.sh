#!/usr/bin/env bash

mvn clean install

declare -a model_replicas=(1 2 4 8 16)
for replicas in "${model_replicas[@]}"; do
  ./experiment_sca.sh 1 $replicas
done
sleep 10
