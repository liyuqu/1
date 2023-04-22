#!/usr/bin/env bash

mvn clean install
./experiment.sh 1

sleep 10

./experiment.sh 2
sleep 10

./experiment.sh 3

sleep 10
./experiment.sh 4

sleep 10
~
~