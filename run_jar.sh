lib=$(echo ./target/lib/*.jar | tr ' ' ',')
spark-submit --master yarn --deploy-mode client  --class historyAnalysis.UserProfiling \
--packages mysql:mysql-connector-java:5.1.26,log4j:log4j:1.2.17,org.json:json:20160212 \
 ./target/recommendation-2.0-SNAPSHOT.jar
