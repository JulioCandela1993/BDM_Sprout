
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
                new UsernamePasswordCredentials("elastic", "Ta5MiyyJBclJxwuv0bbeFWXr"));

        RestClientBuilder builder = RestClient.builder(new HttpHost(
                "532aba02e2cb444e97a5176e981287fe.europe-west1.gcp.cloud.es.io", 9243, "https"))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        RestHighLevelClient client = new RestHighLevelClient(builder);

        Map<String,Object> map = new HashMap<>();
        //You can convert any Object.
        map.put("name", "kimchy");
        map.put("userId", "trying out Elasticsearch");

        String json = new ObjectMapper().writeValueAsString(map);

        IndexRequest request = new IndexRequest("users");
        request.id("12337");
        request.source(json, XContentType.JSON);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println("response id: "+indexResponse.getId());

        client.close();

    }

}
