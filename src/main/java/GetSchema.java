import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.avro.Schema;

import java.io.IOException;

public class GetSchema {

    public static Schema getSchema() throws IOException {
        OkHttpClient httpClient = new OkHttpClient();
        Request request = new Request.Builder().url("http://localhost:8081/subjects/test-topic-value/versions/1").build();
        String output = httpClient.newCall(request).execute().body().string();
        // Parse in json ans return schema attribute
//        {"subject":"test-topic-value","version":2,"id":2,"schema":"{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.avro.producer\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}"}
        // "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.avro.producer\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}

        JsonObject jsonObject = new JsonParser().parse(output).getAsJsonObject();
        String newOutput = jsonObject.get("schema").getAsString();

        //Schema schema = Schema.parse(newOutput);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(newOutput);

        System.out.println(schema);


        return schema;
    }
}
