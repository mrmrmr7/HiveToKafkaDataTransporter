package service.deserizlizer;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import entity.WeatherPerDay;
import jdk.nashorn.internal.parser.TokenType;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class HashMapDeserializer implements Deserializer {
    Gson gson = new Gson();

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        Type type = new TypeToken<HashMap<String, WeatherPerDay>>(){}.getType();
        return gson.fromJson(new String(bytes), type);
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        Type type = new TypeToken<HashMap<String, WeatherPerDay>>(){}.getType();
        return gson.fromJson(new String(data), type);
    }

    @Override
    public void close() {

    }
}
