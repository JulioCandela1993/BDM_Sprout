import com.google.common.io.Files;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.parser.JSONParser;
import scala.Tuple2;
import scala.Tuple4;
import org.json.simple.*;
import scala.Tuple5;

import java.io.File;
import java.io.FileWriter;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Main {

    static String HADOOP_COMMON_PATH = "G:\\Documentos\\MasterDegree\\BDMA\\Classes\\UPC\\SDM\\Project\\DataIntegration\\SparkStreaming\\src\\main\\resources\\winutils";
//    static String HADOOP_COMMON_PATH = "C:\\Users\\Valdemar\\Documents\\Maestria\\ERASMUS\\España\\UPC\\3_BDM\\Project\\SDM_Sprout\\DataIntegration\\SparkStreaming\\src\\main\\resources\\winutils";

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

        JavaDStream<String> filterParser = lines.map(t -> {
            try {
                JSONParser parser = new JSONParser();
                JSONObject json = (JSONObject) parser.parse(t);
                return t;
            }catch(Exception e){
                return "ERROR";
            }
        }).filter(t -> !t.equals("ERROR"));


        JavaPairDStream<Integer, Tuple5<String, String, Integer, String, String>> filteredTexts = filterParser
                .mapToPair(feedbackText ->{
                    JSONParser parser = new JSONParser();
                    JSONObject json = (JSONObject) parser.parse(feedbackText);
                    String customerid = (String) json.get("customerid");
                    String productid = (String) json.get("productid");
                    Long lrating = (long) json.get("rating");
                    int rating = lrating.intValue();
                    String product = (String) json.get("product");
                    String feedback = (String) json.get("about");

                    String lowerCaseText = feedback.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
                    List<String> stopWords = StopWords.getWords();
                    String stemText = lowerCaseText;
                    for (String word : stopWords) {
                        stemText = stemText.replaceAll("\\b" + word + "\\b", "");
                    }
                    return new Tuple2<>(1,new Tuple5<>(customerid, productid, rating, stemText, product));
                });

        JavaDStream<Tuple4<Integer, Tuple4<String, String, Integer, String>, Float, Float>> analysis = filteredTexts.map(t -> {
            String text = t._2._4();
            String[] fdbkWords = text.split(" ");

            int sizeOfFdbk = fdbkWords.length;
            float posWordCount = 0, negWordCount = 0; //declared as double so that scores are not casted to integer

            Set<String> posWords = PositiveWords.getWords();
            Set<String> negWords = NegativeWords.getWords();

            for(String word: fdbkWords){
                if(posWords.contains(word))
                    posWordCount++;

                if(negWords.contains(word))
                    negWordCount++;
            }

            float positiveScore = posWordCount/sizeOfFdbk;
            float negativeScore = negWordCount/sizeOfFdbk;

            return new Tuple4<>(1,new Tuple4<>(t._2._1(),t._2._2(),t._2._3(),t._2._5()), positiveScore, negativeScore);
        });

        JavaDStream<String> result = analysis.map(t -> {
            String classLabel = "";

            if(t._3() > t._4())
                classLabel = "POSITIVE";
            else if(t._3()<t._4())
                classLabel = "NEGATIVE";
            else
                classLabel="NEUTRAL";

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            LocalDateTime now = LocalDateTime.now();
            String date = dtf.format(now);

            String output = "{\"date\":\""+date+"\",\"userid\":"+t._2()._1()+",\"productid\":"+t._2()._2()+",\"rating\":"+t._2()._3()+",\"sentAnalysis\":\""+classLabel+"\"}";


            final CredentialsProvider credentialsProvider =new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials("elastic", "Ta5MiyyJBclJxwuv0bbeFWXr"));

            RestClientBuilder builder = RestClient.builder(new HttpHost(
                    "532aba02e2cb444e97a5176e981287fe.europe-west1.gcp.cloud.es.io", 9243, "https"))
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

            RestHighLevelClient client = new RestHighLevelClient(builder);

            Map<String,Object> map = new HashMap<>();
            //You can convert any Object.
            map.put("date", date);
            map.put("userid", t._2()._1());
            map.put("productid", t._2()._2());
            map.put("rating", t._2()._3());
            map.put("sentAnalysis", classLabel);

            String json = new ObjectMapper().writeValueAsString(map);

            IndexRequest request = new IndexRequest("feedback_web");
            request.id(date.replace(" ","") + "_"+t._2()._1());
            request.source(json, XContentType.JSON);
            client.index(request, RequestOptions.DEFAULT);

            client.close();

            return output;
        });

        result.foreachRDD(rdd -> {
            if(!rdd.isEmpty()){
                List<String> res = rdd.coalesce(1).collect();
                String str = res.get(0) + "\n";

                File file = new File("CustomerFeedback.json");
                FileWriter fr = new FileWriter(file, true);
                fr.write(str);
                fr.close();
            }
        });

        jsc.start();              // Start the computation
        jsc.awaitTermination();   // Wait for the computation to terminate
    }
}
