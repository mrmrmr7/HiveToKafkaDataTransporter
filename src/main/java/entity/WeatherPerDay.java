package entity;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class WeatherPerDay {
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

    public String getDate() {
        return year + "-" + month + "-" + day;
    }
}
