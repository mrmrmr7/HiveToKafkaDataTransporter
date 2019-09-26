package entity;

import com.google.gson.annotations.SerializedName;

public class WeatherPerDayWithPrecision {
    @SerializedName("year")
    private int year;
    @SerializedName("month")
    private int month;
    @SerializedName("day")
    private int day;
    @SerializedName("avg_tmpr_f")
    private double avg_tmpr_f;
    @SerializedName("avg_tmpr_c")
    private double avg_tmpr_c;
    @SerializedName("lng")
    private double lng;
    @SerializedName("lat")
    private double lat;
    @SerializedName("Precision")
    private Precision precision;

    public WeatherPerDayWithPrecision(WeatherPerDay weatherPerDay, Precision precision) {
        this.year = weatherPerDay.getYear();
        this.month = weatherPerDay.getMonth();
        this.day = weatherPerDay.getDay();
        this.avg_tmpr_c = weatherPerDay.getAvg_tmpr_c();
        this.avg_tmpr_f = weatherPerDay.getAvg_tmpr_f();
        this.lat = weatherPerDay.getLat();
        this.lng = weatherPerDay.getLng();
        this.precision = precision;
    }

    @Override
    public String toString() {
        return "WeatherPerDayWithPrecision{" +
                "year=" + year +
                ", month=" + month +
                ", day=" + day +
                ", avg_tmpr_f=" + avg_tmpr_f +
                ", avg_tmpr_c=" + avg_tmpr_c +
                ", lng=" + lng +
                ", lat=" + lat +
                ", precision=" + precision.getPrecision() +
                '}';
    }
}
