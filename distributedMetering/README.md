# Distributed Measurement Framework
## Instructions on how to build and run 
### Run From Command Line (Local Flink Cluster)
1. `$start-cluster.sh`
2. `$flink run meter.jar`
3. Open browser and go to [Flink Web Dashboard](localhost:8081) to see the process.
4. If job has been done, go to `flink-java-project/src/main/output` to see the result and you can also find logs at `flink-java-project/logs/log.log`.
5. `$stop-cluster.sh`
***
### Run From IDE
1. Import this repo as Maven project.
2. Run `flink-java-project/src/main/java/org/apache/flink/quickstart/App.java`
3. If job has been done, go to `flink-java-project/src/main/output` to see the result and you can also find logs at `flink-java-project/logs/log.log`.

