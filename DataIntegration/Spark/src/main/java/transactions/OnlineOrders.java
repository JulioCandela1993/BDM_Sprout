package transactions;

import com.github.javafaker.Faker;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class OnlineOrders {

    public static String transformOrders(JavaSparkContext spark) {
        String out = "";
        double por_comission = 0.9;
        int delivery_fee = 3;
        int numnames = 1000;
        String[] sex = {"Male","Female"};

        Faker faker = new Faker();
        Random rand = new Random();
        ArrayList<String> names = new ArrayList<String>();
        for(int i = 0;i<numnames;i++){
            names.add(faker.artist().name());
        }

        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm", Locale.ENGLISH);
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

        /*********Spark starts here************/

        JavaRDD<String> orders = spark.textFile("src/main/resources/Online_Retail.csv");

        JavaRDD<String> dataRDD = orders.filter(t -> !t.contains("InvoiceNo") && !t.split(",")[6].equals(""));

        JavaRDD<String> dataRDDcache = dataRDD.cache();


        //Order Node
        JavaRDD<String> orderNode = dataRDDcache.mapToPair(l -> new Tuple2<String,Double>(
                l.split(",")[0]
                , Double.parseDouble(l.split(",")[3]) * Double.parseDouble(l.split(",")[5])))
                .reduceByKey( (x,s) -> x +s ) // Total Price of the order
                //.filter(f->f._2>=0); // Orders with values more than 0;
                .map(f-> "O"+f._1 +";" + f._2+";" + f._2 *por_comission +";"+delivery_fee);
        JavaRDD<String> headerordernode = spark.parallelize(Arrays.asList(":ID;total_price:double;total_cost:double;delivery:double"));

        headerordernode.union(orderNode).repartition(1).saveAsTextFile("src/main/resources/orderNode");

        // in_date edge
        JavaRDD<String> indateEdge = dataRDDcache.mapToPair(l -> new Tuple2<String, Date>(
                l.split(",")[0], formatter.parse(l.split(",")[4])))
                .reduceByKey((x,s) ->    x.after(s) ? x : s)
                .map(f-> "O"+f._1 +";" + dateFormat.format(f._2));
        JavaRDD<String> headerindateedge = spark.parallelize(Arrays.asList(":START_ID;:END_ID"));

        headerindateedge.union(indateEdge).repartition(1).saveAsTextFile("src/main/resources/indateEdge");

        // Supplied by Edge
        JavaRDD<String> suppliedbyEdge = dataRDDcache.map(l->l.split(",")[0])
                .distinct()
                .map(f-> "O"+f +";" + "F" + rand.nextInt(50));
        JavaRDD<String> headersuppliedbyedge = spark.parallelize(Arrays.asList(":START_ID;:END_ID"));

        headersuppliedbyedge.union(suppliedbyEdge).repartition(1).saveAsTextFile("src/main/resources/suppliedbyEdge");

        //Products node
        JavaRDD<String> productNode = dataRDDcache.mapToPair(l -> new Tuple2<String,String>(l.split(",")[1]
                , l.split(",")[2]))
                .distinct()
                .reduceByKey((x,s) -> x.isEmpty() || x.length()== 0 ? s : x)
                .map(f-> "P" + f._1+";" +f._2);
        JavaRDD<String> headerproductnode = spark.parallelize(Arrays.asList(":ID;nameproduct:string"));

        headerproductnode.union(productNode).repartition(1).saveAsTextFile("src/main/resources/productNode");

        //belongsto edge
        JavaRDD<String> belongstoEdge = dataRDDcache.map(l->l.split(",")[1]).distinct()
                .map(f-> "P" + f+";" + "CAT" + rand.nextInt(4));
        JavaRDD<String> headerbelongstoedge = spark.parallelize(Arrays.asList(":START_ID;:END_ID"));

        headerbelongstoedge.union(belongstoEdge).repartition(1).saveAsTextFile("src/main/resources/belongstoEdge");

        //contains edge
        JavaRDD<String> containsEdge = dataRDDcache
                .map(f-> "O" + f.split(",")[0]+";" + "P" + f.split(",")[1] + ";" + f.split(",")[3]
                        +  ";" + f.split(",")[5] + ";" + (Double.parseDouble(f.split(",")[5]) * por_comission ));
        JavaRDD<String> headercontainsedge = spark.parallelize(Arrays.asList(":START_ID;:END_ID;quantity:int;unitprice:double;costprice:double"));

        headercontainsedge.union(containsEdge).repartition(1).saveAsTextFile("src/main/resources/containsEdge");

        /*Customer Order Transactions*/
        JavaPairRDD<String,String> customerOrder = dataRDDcache.mapToPair(l -> new Tuple2<String,String>(l.split(",")[6] , l.split(",")[0]))
                .distinct();

        JavaPairRDD<String,String> customerOrdercache =customerOrder.cache();

        //Customer Node
        JavaRDD<String> customerNode = customerOrdercache.map(l -> (l._1))
                .distinct()
                .map(f-> "C" + f+";" + names.get(rand.nextInt(numnames)) +";"+  rand.nextInt(100) +";"+ sex[rand.nextInt(1)] );
        JavaRDD<String> headercustomernode = spark.parallelize(Arrays.asList(":ID;name:string;age:int;gender:string"));

        headercustomernode.union(customerNode).repartition(1).saveAsTextFile("src/main/resources/customerNode");

        //buys Edge
        JavaRDD<String> buysEdge = customerOrdercache
                .map(f-> "C" + f._1 +";"+"O"+f._2);
        JavaRDD<String> headerbuysedge = spark.parallelize(Arrays.asList(":START_ID;:END_ID"));

        headerbuysedge.union(buysEdge).repartition(1).saveAsTextFile("src/main/resources/buysEdge");

        // Classified as Edge
        JavaRDD<String> classifiedasEdge = customerOrdercache
                .mapToPair(f->new Tuple2<String,Integer>(f._1, 1))
                .reduceByKey( (x,s) -> x +s )
                .map(f-> "C" + f._1+";" +
                        (f._2<=1 ? "S1" : (f._2<=5 ? "S2" : "S3")));
        JavaRDD<String> headerclassifiedasedge = spark.parallelize(Arrays.asList(":START_ID;:END_ID"));

        headerclassifiedasedge.union(classifiedasEdge).repartition(1).saveAsTextFile("src/main/resources/classifiedasEdge");

        return  "Done";
    }

}
