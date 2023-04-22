1. Run the docker

2. On your LINUX cmd, Run the code following: 

docker run -t --rm -p 8501:8501 --mount type=bind,source=/path/to/team-8/codebase/external-serving/tfserving/models/0_class,target=/models/0_class --mount type=bind,source=/path/to/team-8/codebase/external-serving/tfserving/models/1_class,target=/models/1_class --mount type=bind,source=/path/to/team-8/codebase/external-serving/tfserving/models/odmodel,target=/models/od_class --mount type=bind,source=/path/to/team-8/codebase/external-serving/tfserving/models/models,target=/models/pl_class --mount type=bind,source=/path/to/codebase/external-serving/tfserving/model_config.config,target=/models/model_config.config -t tensorflow/serving --model_config_file=/models/model_config.config

if you run into event loop, the pipeline built on sever. 

3. Run the file in ./codebase

Open start1.sh/start2.sh/start3.sh/start4.sh means Run experiment on pipeline 1, 2, 3, or 4

Each will run five different experiments (each 5 min, but will cost more time since need to write the result out) on both input rate and batch size, output(sink) will be in ./codebase/experiments-results

You can change the experiment configuration in ./codebase/src/main/java/expconfigs

common.properties can change experiment duration for each experiment

input-rate/batch-size can change the config to run different configuration's experiment