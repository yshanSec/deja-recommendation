package historyAnalysis;

import common.ConfFromProperties;
import common.ProductSearcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

/**
 * Created by yishan on 1/8/16.
 */
public class UserProfiling {
    private static ConfFromProperties userProfilingConf;
    private static String master;
    private static String productLoggerPath;
    private static String keywordsLoggerPath;
    private static String storePath;
    private static String hdfsURI;
    public static Broadcast<ProductSearcher> productSearcher;

    private static void loadConf() throws IOException{
//        get configuration file
//        UserProfiling.baseDir = System.getProperty("user.dir");
        String propertiesDefaultSuffix = ".properties";
        String sparkPropertyFile = UserProfiling.class.getName().concat(propertiesDefaultSuffix);
        UserProfiling.userProfilingConf = new ConfFromProperties(sparkPropertyFile);
    }

    private static void init() throws IOException{
        UserProfiling.productLoggerPath = userProfilingConf.getValue("productLoggerPath");
        UserProfiling.keywordsLoggerPath = userProfilingConf.getValue("keywordsLoggerPath");
        UserProfiling.master = userProfilingConf.getValue("master");
        UserProfiling.storePath = userProfilingConf.getValue("storePath");
        UserProfiling.hdfsURI = userProfilingConf.getValue("hdfsURI");
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem hdfs = FileSystem.get(URI.create(UserProfiling.hdfsURI), conf);
        hdfs.delete(new Path(storePath), true);
    }

    public static void main(String[] argv) throws IOException{
        UserProfiling.loadConf();
        UserProfiling.init();

        SparkConf conf = new SparkConf().setAppName(UserProfiling.class.getName()).setMaster(master);
        JavaSparkContext sparkcontext = new JavaSparkContext(conf);
        //broadcast value
        UserProfiling.productSearcher = sparkcontext.broadcast(new ProductSearcher(UserProfiling.userProfilingConf));
        JavaRDD<String> userProduceLoggerFileRDD = sparkcontext.textFile(UserProfiling.productLoggerPath);
        JavaRDD<String> userKeywordLoggerFileRDD = sparkcontext.textFile(UserProfiling.keywordsLoggerPath);
        JavaPairRDD<String, Integer> elementProducePairs = userProduceLoggerFileRDD.flatMapToPair(new ProductElementsFlatMap());
        JavaPairRDD<String, Integer> elementKeywordPairs = userKeywordLoggerFileRDD.flatMapToPair(new FilterElementFlatMap());
        JavaPairRDD<String, Integer> elementPairs = elementProducePairs.union(elementKeywordPairs);

        JavaPairRDD<String, Integer> elementGatherPairs = elementPairs.reduceByKey(new ProductElementsReduce());
        JavaPairRDD<String, HashMap<String, HashMap<String, Integer>>> userInfoPair = elementGatherPairs.mapToPair(new UserElementMap());
        JavaPairRDD<String, HashMap<String, HashMap<String, Integer>>> userInfo = userInfoPair.reduceByKey(new UserElementReduce());
        userInfo.saveAsTextFile(UserProfiling.storePath);
    }
}