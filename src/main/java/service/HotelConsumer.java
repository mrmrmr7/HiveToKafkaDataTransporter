package service;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class HotelConsumer {
    private static final String KAFKA_SERVER_URL = "192.168.99.100";
    private static final String KAFKA_SERVER_PORT = "6667";
    private static final String HOTEL_METADATA = "src/main/resources/hotelMetadata.txt";
    private static final String topic = "BigData";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "client_1");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2494");
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (int) FileUtils.ONE_MB);

        KafkaConsumer consumer = new KafkaConsumer(properties);
        consumer.subscribe(Collections.singletonList(topic));
        Gson gson = new Gson();
        int oldCounter = 0;
        int newCounter = 0;
        List<String> hotelJson = new ArrayList<>();

        try {
            File hotelMetadata = new File(HOTEL_METADATA);
            hotelMetadata.createNewFile();
            FileWriter fileWriter = new FileWriter(HOTEL_METADATA, false);

            do {
                oldCounter = newCounter;
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMinutes(1));

                for (ConsumerRecord<Integer, String> record : records) {
                    hotelJson.add(record.value());
                    newCounter++;
                }

                System.out.println("Hotel count: " + (newCounter - oldCounter));
            } while (oldCounter != newCounter);

            System.out.println("Total read: " + newCounter);
            hotelJson.forEach( (each) -> {
                        try {
                            fileWriter.write(each);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
            );

        } catch (IOException e) {
            System.out.println("Can not create hotelMetadata.txt");
        }

    }
}