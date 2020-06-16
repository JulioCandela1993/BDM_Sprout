package customerfeedback;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class CustomerFeedback {
    public static String generateCustomerFeedbackFile(JavaSparkContext ctx) {
        String out = "Let's-a go!";

        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Example - Read JSON to RDD")
                .master("local[2]")
                .getOrCreate();

        // read list to RDD
        String jsonPath = "src/main/resources/CustomerFeedback.json";
        JavaRDD<Row> items = spark.read().json(jsonPath).toJavaRDD();
        JavaRDD<String> feedbackEdge = items.map(t -> {
            String date = t.getAs("date").toString().replace("-","");
            String feedback = "C"+t.getAs("userid")+";"+"P"+t.getAs("productid")+";"+t.getAs("rating")+";"+t.getAs("sentAnalysis")+";"+date;
            return feedback;
        });
        JavaRDD<String> headerFeedbackEdge = ctx.parallelize(Arrays.asList(":START_ID;:END_ID;rating:int;sentimentalAnalysis:string;date:int"));

        headerFeedbackEdge.union(feedbackEdge).repartition(1).saveAsTextFile("src/main/resources/feedbackEdge");

        return out;
    }
}
