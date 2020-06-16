package marketprices;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.github.javafaker.Faker;

public class MarketPrices {
    public static String marketPriceCalculation(JavaSparkContext ctx) {
        String out = "Let's-a go!";

        JavaRDD<String> categoryNode = ctx.textFile("fixedData/CategoryNode.csv");
        JavaRDD<String> listProducts = ctx.textFile("src/main/resources/ListProducts.txt");
        JavaRDD<String> municipalities = ctx.textFile("src/main/resources/MunicipalitiesBarcelona.txt");
        JavaRDD<String> animal = ctx.textFile("src/main/resources/market-prices-animal-products.csv");
        JavaRDD<String> dairy = ctx.textFile("src/main/resources/market-prices-dairy-products.csv");
        JavaRDD<String> fruit = ctx.textFile("src/main/resources/market-prices-fruit-products.csv");
        JavaRDD<String> vegetable = ctx.textFile("src/main/resources/market-prices-vegetable-products.csv");
        JavaRDD<String> vegetal = ctx.textFile("src/main/resources/market-prices-vegetal-products.csv");

        JavaRDD<String> allPrices = animal.union(dairy).union(fruit).union(vegetable).union(vegetal);
        JavaRDD<String> allPricesFiltered = allPrices
                .filter(t -> !t.contains(",Date,") && t.split(",").length > 12)
                .filter(t -> !t.split(",")[12].equals(""));
        JavaPairRDD<String,Tuple2<String,Double>> spainPrices = allPricesFiltered.mapToPair(t -> new Tuple2<>(t.split(",")[0],new Tuple2<>(t.split(",")[1]+"/01",Double.parseDouble(t.split(",")[12]))));
        JavaPairRDD<String,Tuple2<String,Double>> spainPricesCache = spainPrices.cache();

        JavaPairRDD<String,String> productID = listProducts
                .filter(t -> !t.contains("ProductID"))
                .mapToPair(t -> new Tuple2<>(t.split(",")[1], "P"+t.split(",")[0]));
        JavaPairRDD<String,String> productIDCache = productID.cache();

        // Product Node
        JavaPairRDD<String,Tuple2<String,Double>> latestPrice = spainPricesCache
                .reduceByKey((a, b) -> {
                    Date dateA = new SimpleDateFormat("yyyy/MM/dd").parse(a._1);
                    Date dateB = new SimpleDateFormat("yyyy/MM/dd").parse(b._1);
                    if(dateA.after(dateB)) return a;
                    else return b;
                });
        JavaPairRDD<String,Tuple2<Tuple2<String,Double>,String>> latestPriceWithID = latestPrice.join(productIDCache);
        JavaRDD<String> productNode = latestPriceWithID.map(t -> t._2._2+";"+t._1+";"+t._2._1._2);
        JavaRDD<String> headerProductNode = ctx.parallelize(Arrays.asList(":ID;nameproduct:string;marketpriceEU:double"));

        headerProductNode.union(productNode).repartition(1).saveAsTextFile("src/main/resources/productNode");

        // price_in_date edge
        JavaPairRDD<String,Tuple2<Tuple2<String,Double>,String>> spainPricesWithID = spainPricesCache.join(productIDCache);
        JavaRDD<String> priceInDateEdge = spainPricesWithID.map(t -> t._2._2+";"+t._2._1._1.replace("/","")+";"+t._2._1._2);
        JavaRDD<String> headerPriceInDateEdge = ctx.parallelize(Arrays.asList(":START_ID;:END_ID;marketpriceEU:double"));

        headerPriceInDateEdge.union(priceInDateEdge).repartition(1).saveAsTextFile("src/main/resources/priceInDateEdge");

        //  belongs_to edge
        JavaPairRDD<String,String> productID_Category = listProducts
                .filter(t -> !t.contains("ProductID"))
                .mapToPair(t -> new Tuple2<>(t.split(",")[2], "P"+t.split(",")[0]));
        JavaPairRDD<String,String> categoryID = categoryNode
                .filter(t -> !t.contains("categoryname"))
                .mapToPair(t -> new Tuple2<>(t.split(";")[1], "P"+t.split(";")[0]));
        JavaPairRDD<String,Tuple2<String,String>> productCategoryJoin = productID_Category.join(categoryID);
        JavaRDD<String> belongsToEdge = productCategoryJoin.map(t -> t._2._1+";"+t._2._2);
        JavaRDD<String> headerBelongsToEdge = ctx.parallelize(Arrays.asList(":START_ID;:END_ID"));

        headerBelongsToEdge.union(belongsToEdge).repartition(1).saveAsTextFile("src/main/resources/belongsToEdge");

        // Farmer Node
        int numNames = 50;
        Faker faker = new Faker();
        Random rand = new Random();
        ArrayList<String> names = new ArrayList<>();
        for(int i = 0;i<numNames;i++){
            names.add("F"+i+";"+faker.artist().name());
        }
        JavaRDD<String> farmerNames = ctx.parallelize(names);

        List<String> barMunicipalities = municipalities
                .filter(t -> !t.contains("Municipality"))
                .map(t -> t.split(",")[0])
                .collect();

        JavaRDD<String> farmerNode = farmerNames.map(t -> t+";"+barMunicipalities.get(rand.nextInt(barMunicipalities.size()))+";"+rand.nextInt(10));
        JavaRDD<String> headerfarmerNode = ctx.parallelize(Arrays.asList(":ID;farmername:string;location:string;experience:int"));

        headerfarmerNode.union(farmerNode).repartition(1).saveAsTextFile("src/main/resources/farmerNode");



//        List<Tuple2<String,Tuple2<Tuple2<String,Double>,String>>> out2 = latestPriceWithID.take(5);
//        List<Tuple2<String,Tuple2<String,Double>>> out2 = latestPrice.take(5);

//        JavaRDD<String> dataRDD = orders.filter(t -> !t.contains("InvoiceNo") && !t.split(",")[6].equals(""));
//        JavaRDD<String> dataRDDcache = dataRDD.cache();

        return out;
    }
}
