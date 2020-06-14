package exercise_1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.execution.columnar.DOUBLE;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import shaded.parquet.org.apache.thrift.TUnion;

import java.util.List;

public class Exercise_1 {


    public static String humanResources(JavaSparkContext spark) {
        String out = "";

        JavaRDD<String> breastRDD = spark.textFile("src/main/resources/HR_comma_sep.csv");

        JavaRDD<String> dataRDD = breastRDD.filter(t -> !t.contains("satisfaction_level"));

        JavaPairRDD<String, Double> pairRDD = dataRDD.mapToPair(f -> new Tuple2<String, Double>(f.split(",")[2]
                                                        , Double. parseDouble(f.split(",")[0])));

        // AggregatebyKey is the most efficient way to perform an Average since it includes a combiner and it only needs to shuffle
        List<Tuple2<Double,String>> satisfactionList = pairRDD
                                //Acumulator s(Sum , Counter)
                .aggregateByKey(new Tuple2<Double, Integer>(0.0,0) , (s,x) -> new Tuple2<Double, Integer>(s._1 +x , s._2 +1) ,
                        (s1,s2) -> new Tuple2<Double, Integer>(s1._1+ s2._1 , s1._2 + s2._2))
                .mapToPair(s -> new Tuple2<Double, String>(s._2._1/s._2._2,s._1)).sortByKey(true).collect();

        for (Tuple2<Double, String> s : satisfactionList) {
            out += "Employees who work in "+ s._2 + " projects have an average satisfaction of "+ s._1;
            out += "\n";
        }

        return out;
    }

}

