import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import transactions.OnlineOrders;
import marketprices.MarketPrices;
import customerfeedback.CustomerFeedback;

public class Main {
	
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));

        SparkConf conf = new SparkConf().setAppName("Lab6_Spark").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
		
		if (args.length < 1) {
			throw new Exception("Wrong number of parameters, usage: (orders,mkt,feedback)");
		}

		if (args[0].equals("orders")) {
            System.out.println(OnlineOrders.transformOrders(ctx));
        }else if (args[0].equals("mkt")) {
			System.out.println(MarketPrices.marketPriceCalculation(ctx));
		}else if (args[0].equals("feedback")) {
			System.out.println(CustomerFeedback.generateCustomerFeedbackFile(ctx));
		}		else {
			throw new Exception("Wrong argument name");
		}
	}
}

