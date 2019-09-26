package service.serializer;

import com.google.gson.Gson;
import entity.WeatherPerDay;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class HashMapSerializer implements Serializer<HashMap<String, WeatherPerDay>> {
    Gson gson = new Gson();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, HashMap<String, WeatherPerDay> map) {
        return gson.toJson(map).getBytes();
    }

    @Override
    public byte[] serialize(String topic, Headers headers, HashMap<String, WeatherPerDay> data) {
        return gson.toJson(data).getBytes();
    }

    @Override
    public void close() {

    }
}
