#!/usr/bin/env bash
lib=$(echo ./target/lib/*.jar | tr ' ' ',')
spark-submit --master yarn --deploy-mode cluster  --class historyAnalysis.UserProfiling \
--packages mysql:mysql-connector-java:5.1.26\
 ./target/recommendation-2.0-SNAPSHOT.jar
