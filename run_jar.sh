lib=$(echo ./target/lib/*.jar | tr ' ' ',')
spark-submit --master yarn-cluster --class historyAnalysis.UserProfiling --jars $lib \
./target/recommendation-2.0-SNAPSHOT.jar
