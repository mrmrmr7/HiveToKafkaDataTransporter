package service;

import ch.hsr.geohash.GeoHash;
import com.google.common.primitives.Bytes;
import com.google.gson.Gson;
import entity.*;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.SessionStore;
import scala.collection.immutable.Stream;
import service.deserizlizer.HashMapDeserializer;
import service.impl.HotelToWeatherMapper;
import service.serializer.HashMapSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class KStreamMapper {
    private static final String KAFKA_SERVER_URL = "192.168.99.100";
    private static final String KAFKA_SERVER_PORT = "6667";
    private static final String PATH = "src/main/resources/";
    private static final String HOTEL_METADATA = PATH + "hotelMetadata.txt";
    private static String IN_TOPIC = "weather_data";
    private static String OUT_TOPIC = "weather_data_mapped";

    public static void main(String[] args) {
        String pathToHotelsData = args.length > 0 ? args[0] : HOTEL_METADATA;
        String inTopicName = args.length > 1 ? args[1] : IN_TOPIC;
        String outTopicName = args.length > 2 ? args[2] : OUT_TOPIC;
        KafkaStreams kafkaStreams = createKafkaStreams(pathToHotelsData, inTopicName, outTopicName);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public static KafkaStreams createKafkaStreams(String pathToHotelsData, String inTopic, String outTopic) {
        Gson gson = new Gson();
        List<Hotel> hotelList = new ArrayList<>();
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(pathToHotelsData));
            File file = new File(pathToHotelsData);
            String line = reader.readLine();
            hotelList.add(gson.fromJson(line, Hotel.class));
            while (line != null) {
                // read next line
                hotelList.add(gson.fromJson(line, Hotel.class));
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka_2016_10_01");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:6667");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(3 * FileUtils.ONE_GB));
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, String.valueOf(Integer.MAX_VALUE));

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streams = builder.stream(inTopic);
        Serde<HashMap<String, WeatherPerDay>> hashMapSerde = Serdes.serdeFrom(new HashMapSerializer(), new HashMapDeserializer());

        streams
                .selectKey((key, value) -> {
                    WeatherPerDay weatherPerDay = gson.fromJson(value, WeatherPerDay.class);
                    return weatherPerDay.getDate();
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                //.windowedBy(SessionWindows.with(Duration.ofMinutes(5)))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(10)))
                .aggregate(
                        HashMap<String, WeatherPerDay>::new,
                        (k, v, map) -> {
                            WeatherPerDay value = gson.fromJson(v, WeatherPerDay.class);
                            String geoHash = GeoHash.geoHashStringWithCharacterPrecision(
                                    value.getLat(),
                                    value.getLng(),
                                    5);
                            map.put(geoHash, value);
                            return map;
                        },
//                        (aggKey, leftAggValue, rightAggValue) -> {
//                            HashMap<String, WeatherPerDay> hashMap = new HashMap<>();
//                            hashMap.putAll(leftAggValue);
//                            hashMap.putAll(rightAggValue);
//                            return hashMap;
//                        },
                        Materialized
                                .with(Serdes.String(), hashMapSerde))
                .mapValues((value) -> {
                    HotelToWeatherMapper mapper = new HotelToWeatherMapper();
                    return mapper.map(value, hotelList);
                })
                .toStream()
                .flatMap((key, value) -> {
                    List<KeyValue<Hotel, Pair<WeatherPerDay, Precision>>> pairs = new LinkedList<>();
                    for(Map.Entry<Hotel, Pair<WeatherPerDay, Precision>> entry : value.entrySet()) {
                        pairs.add(KeyValue.pair(entry.getKey(), entry.getValue()));
                    }
                    return pairs;
                })
                .map( (key, value) -> {
                    String hotel = gson.toJson(key);
                    WeatherPerDay weatherPerDay = value.getKey();
                    Precision precision = value.getValue();
                    WeatherPerDayWithPrecision res = new WeatherPerDayWithPrecision(weatherPerDay, precision);
                    String weather = gson.toJson(res.toString());
                    return KeyValue.pair(hotel, weather);
                })
                .to(outTopic);
        return new KafkaStreams(builder.build(), properties);
    }
}
