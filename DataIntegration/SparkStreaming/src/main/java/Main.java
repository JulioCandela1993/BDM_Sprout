import com.google.common.io.Files;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class Main {

    static String HADOOP_COMMON_PATH = "C:\\Users\\Valdemar\\Documents\\Maestria\\ERASMUS\\España\\UPC\\3_BDM\\Lab6\\Lab06-Training_Java\\src\\main\\resources\\winutils";

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
        SparkConf conf = new SparkConf().setAppName("SparkStreamingTraining").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaStreamingContext jsc = new JavaStreamingContext(ctx, new Duration(1000));
        LogManager.getRootLogger().setLevel(Level.ERROR);
        LogManager.shutdown();
        jsc.checkpoint(Files.createTempDir().getAbsolutePath());

        // Create a DStream that will connect to hostname:port, like localhost:9999
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);
        // Split each line into words
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);
        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();

        jsc.start();              // Start the computation
        jsc.awaitTermination();   // Wait for the computation to terminate
    }
}
