package service.impl;

import entity.Hotel;
import entity.Pair;
import entity.Precision;
import entity.WeatherPerDay;

import java.util.*;

public class HotelToWeatherMapper {
    public Map<Hotel, Pair<WeatherPerDay, Precision>> map(
            Map<String, WeatherPerDay> weatherPerDayMap,
            List<Hotel> hotelList) {

        System.out.println("Weather count: " + weatherPerDayMap.size());
        System.out.println("Hotels count: " + hotelList.size());

        Map<Hotel, Pair<WeatherPerDay, Precision>> res = new HashMap<>();

        for (Hotel hotel : hotelList) {

            Pair<WeatherPerDay, Precision> suitableWeatherWithPrecision = null;
            if (weatherPerDayMap.containsKey(hotel.getGeoHash())) {
                WeatherPerDay accurateWeather = weatherPerDayMap.get(hotel.getGeoHash());
                suitableWeatherWithPrecision = new Pair<>(accurateWeather, Precision.FIVE);
            } else {
                WeatherPerDay weatherPerDay = findWeatherByGeoHashWithPrecision(
                        weatherPerDayMap,
                        hotel,
                        Precision.FOUR
                );

                if (weatherPerDay != null) {
                    suitableWeatherWithPrecision = new Pair<>(weatherPerDay, Precision.FOUR);
                } else {
                    weatherPerDay = findWeatherByGeoHashWithPrecision(
                            weatherPerDayMap,
                            hotel,
                            Precision.THREE
                    );

                    if (weatherPerDay != null) {
                        suitableWeatherWithPrecision = new Pair<>(weatherPerDay, Precision.THREE);
                    } else {
                        System.out.println("Could not find suitable weather for hotel: " + hotel.toString());
                    }
                }
            }

            if (suitableWeatherWithPrecision != null) {
                System.out.println("Hotel: " + hotel);
                System.out.println("Date: " + suitableWeatherWithPrecision.getKey().getDate());
                System.out.println("Weather: " + suitableWeatherWithPrecision.getKey());
                System.out.println("Precision: " + suitableWeatherWithPrecision.getValue());
                res.put(hotel, suitableWeatherWithPrecision);
            }
        }

        return res;
    }

    private WeatherPerDay findWeatherByGeoHashWithPrecision(
            Map<String, WeatherPerDay> map,
            Hotel hotel,
            Precision precision) {
        WeatherPerDay res = null;

        for (String key : map.keySet()) {
            if (key.startsWith(hotel.getGeoHash().substring(0, precision.getPrecision()))) {
                res = map.get(key);
            }
        }

        return res;
    }
}
