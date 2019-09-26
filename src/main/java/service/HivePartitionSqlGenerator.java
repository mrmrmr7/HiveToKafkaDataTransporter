package service;

import org.omg.PortableInterceptor.LOCATION_FORWARD;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.stream.IntStream;

public class HivePartitionSqlGenerator {
    private static final String ALTER_TABLE      = "ALTER TABLE ";
    private static final String TABLE_NAME       = "data ADD ";
    private static final String PARTITION_FORMAT = "PARTITION (year={0}, month={1}, day={2}) ";
    private static final String LOCATION         = "/user/hive/kafka/weather/";
    private static final String PATH_FORMAT      = "year={0}/month={1}/day={2}";
    private static final String EOQ              = "; \n";

    public static void main(String[] args) {
        String location = MessageFormat
                .format("LOCATION ''''{0}{1}''''", args.length > 0 ? args[0] : LOCATION, PATH_FORMAT);
        int yearStart =     args.length > 1 ? Integer.parseInt(args[1]) : 2017;
        int monthStart =    args.length > 2 ? Integer.parseInt(args[2]) : 8;
        int monthEnd =      args.length > 3 ? Integer.parseInt(args[3]) : 9;
        int dayStart =      args.length > 4 ? Integer.parseInt(args[4]) : 1;
        int dayEnd =        args.length > 5 ? Integer.parseInt(args[5]) : 31;

        StringBuilder stringBuilder = new StringBuilder();

        int[] YEAR_ARRAY = new int[]{yearStart};
        final int[] MONTH_ARRAY = IntStream.rangeClosed(monthStart, monthEnd).toArray();
        final int[] DAY_ARRAY = IntStream.rangeClosed(dayStart, dayEnd).toArray();

        Arrays.stream(YEAR_ARRAY).forEach(
                (year) -> Arrays.stream(MONTH_ARRAY).forEach(
                        (month) -> Arrays.stream(DAY_ARRAY).forEach(
                                (day) -> {
                                    stringBuilder.append(ALTER_TABLE + TABLE_NAME);
                                    stringBuilder.append(MessageFormat.format(PARTITION_FORMAT, String.valueOf(year), month, day));
                                    stringBuilder.append(MessageFormat.format(location, String.valueOf(year), month, day));
                                    stringBuilder.append(EOQ);
                                })
                )
        );


        System.out.println(stringBuilder.toString());
    }
}
