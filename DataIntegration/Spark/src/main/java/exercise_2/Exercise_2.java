package exercise_2;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


public class Exercise_2 {

    //The happiest country of Europe in 2015, 2016 and 2017 according to its happiness score
    public static String happinessRanking(JavaSparkContext spark) {
        String out = "";

        JavaRDD<String> report_2015 = spark.textFile("src/main/resources/2015.csv");
        JavaRDD<String> report_2016 = spark.textFile("src/main/resources/2016.csv");
        JavaRDD<String> report_2017 = spark.textFile("src/main/resources/2017.csv");

        JavaRDD<String> all = report_2015.union(report_2016).union(report_2017);

        JavaRDD<String> unified = all.repartition(1);

        List<Tuple2<Double,String>> ranking = unified.flatMap(t -> {
            if (t.contains("Country")) return new ArrayList<String>().iterator();
            else return Lists.newArrayList(t).iterator();
          })
          .mapToPair(t -> new Tuple2<Double,String>(Double.parseDouble(t.split(",")[3]),t))
          .groupByKey()
          .sortByKey(false)
          .mapToPair(t -> new Tuple2<Double,String>(t._1, t._2.iterator().next()))
          .mapValues(t -> t.contains("Europe") ? t : null)
          .collect();

        Tuple2<Double,String> top = ranking.get(0);
        out = top._2.split(",")[0]+" is the happiest country in Europe for 2015, 2016 and 2017 with an score of "+top._1;

        return out;
    }

}
