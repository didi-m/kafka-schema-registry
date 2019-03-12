import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.IOException;
import java.util.Properties;

public class ProducerExample {

    public static void main(String[] str) throws IOException {
        System.out.println("Starting ProducerAvroExample ...");
        sendMessages();
    }
    private static void sendMessages() throws IOException {
        Producer<String, byte[]> producer = createProducer();
        sendRecords(producer);
    }
    private static Producer<String, byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DefaultSerializers.StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

    }
    private static void sendRecords(Producer<String, byte[]> producer) throws IOException {
        String topic = "test-topic";
        int partition = 0;
        while (true) {
            for (int i = 1; i < 100; i++)
                producer.send(new ProducerRecord<>(topic, partition,
                        Integer.toString(0), record(i + "", i + 3.44)));

        }
    }
    private static byte[] record(String id, Double amount) throws IOException{
        GenericRecord record = new GenericData.Record(GetSchema.getSchema());
        record.put("id", id);
        record.put("amount", amount);
        return AvroSupport.dataToByteArray(GetSchema.getSchema(), record);
    }
}