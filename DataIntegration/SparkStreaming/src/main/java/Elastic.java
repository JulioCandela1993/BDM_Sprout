
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;


import scala.Tuple2;
import twitter4j.JSONObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Elastic {


    public static void main(String[] args) throws Exception {

        final CredentialsProvider credentialsProvider =new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "AmFWN6MOtCWSDmqiOljKm4TA"));

        RestClientBuilder builder = RestClient.builder(new HttpHost(
                "b0fe863172b24fc793f34d35683b658e.europe-west1.gcp.cloud.es.io", 9243, "https"))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        System.out.println("Yes");

        RestHighLevelClient client = new RestHighLevelClient(builder);

        System.out.println("Yes");

        CreateIndexRequest request = new CreateIndexRequest("feedback_web");
        request.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 2)
        );
        Map<String, Object> messagetext = new HashMap<>();
        messagetext.put("type", "string");
        Map<String, Object> messagedate = new HashMap<>();
        messagedate.put("type", "date");
        Map<String, Object> messagelong = new HashMap<>();
        messagelong.put("type", "long");
        Map<String, Object> properties = new HashMap<>();

        properties.put("date", messagedate);
        properties.put("userid", messagetext);
        properties.put("productid", messagetext);
        properties.put("rating", messagelong);
        properties.put("sentAnalysis", messagetext);

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);
        request.mapping(mapping);
        CreateIndexResponse indexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println("response id: "+indexResponse.index());


    }

}
