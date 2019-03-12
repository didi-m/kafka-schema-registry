import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerExample {
    public static void main(String[] str) throws IOException {

        System.out.println("Starting ConsumerExample ...");
        readMessages();
    }
    private static void readMessages() throws IOException {

        KafkaConsumer<String, byte[]> consumer = createConsumer();
        // Assign to specific topic and partition.
        consumer.assign(Arrays.asList(new TopicPartition("test-topic", 0)));
        processRecords(consumer);
    }
    private static void processRecords(KafkaConsumer<String, byte[]> consumer) throws IOException {

        Schema schema = GetSchema.getSchema();

        while (true) {

            ConsumerRecords<String, byte[]> records = consumer.poll(100);
            long lastOffset = 0;

            for (ConsumerRecord<String, byte[]> record : records) {

                System.out.println(record.value());

                GenericRecord genericRecord = AvroSupport.byteArrayToData(schema, record.value());
                String id = getValue(genericRecord, "id", String.class);
                System.out.printf("\n\roffset = %d, key = %s, value = %s", record.offset(), record.key());
                lastOffset = record.offset();
            }

            System.out.println("lastOffset read: " + lastOffset);
            consumer.commitSync();
        }
    }

    private static KafkaConsumer<String, byte[]> createConsumer() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "12");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        return new KafkaConsumer<>(props);
    }
}