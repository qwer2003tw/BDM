package homework;

import java.io.Serializable;
import java.util.regex.Pattern;

public class Data implements Serializable {
    public final String date;
    public final String time;
    public final float global_active_power;
    public final float global_reactive_power;
    public final float voltage;
    public final float global_intensity;
    public final float sub_metering_1;
    public final float sub_metering_2;
    public final float sub_metering_3;
    public final boolean isInvalid;
    public final String id;
    private final static String DELIMITER = ";";
    private String _string = null;

    Data(
            String id,
            String date, String time,
            float global_active_power, float global_reactive_power,
            float voltage,
            float global_intensity, float sub_metering_1, float sub_metering_2, float sub_metering_3,
            boolean isInvalid
    ) {
        this.id = id;
        this.date = date;
        this.time = time;
        this.global_active_power = global_active_power;
        this.global_reactive_power = global_reactive_power;
        this.voltage = voltage;
        this.global_intensity = global_intensity;
        this.sub_metering_1 = sub_metering_1;
        this.sub_metering_2 = sub_metering_2;
        this.sub_metering_3 = sub_metering_3;
        this.isInvalid = isInvalid;
    }

    public Data(String data) {
        int asdf;
        String[] dataArr = data.split(Pattern.quote(DELIMITER));
        String date;
        String time;
        float global_active_power;
        float global_reactive_power;
        float voltage;
        float global_intensity;
        float sub_metering_1;
        float sub_metering_2;
        float sub_metering_3;
        boolean valid = false;

        try {
            if (dataArr.length == 9) {
                int idx = 0;
                date = dataArr[idx++];
                time = dataArr[idx++];
                global_active_power = Float.valueOf(dataArr[idx++]);
                global_reactive_power = Float.valueOf(dataArr[idx++]);
                voltage = Float.valueOf(dataArr[idx++]);
                global_intensity = Float.valueOf(dataArr[idx++]);
                sub_metering_1 = Float.valueOf(dataArr[idx++]);
                sub_metering_2 = Float.valueOf(dataArr[idx++]);
                sub_metering_3 = Float.valueOf(dataArr[idx++]);
                valid = true;
            } else {
                throw new Exception("Data error");
            }
        } catch (Exception e) {
            date = "N/A";
            time = "N/A";
            global_active_power = global_reactive_power = voltage = global_intensity = sub_metering_1 = sub_metering_2 = sub_metering_3 = -1;
        }

        this.date = date;
        this.time = time;
        this.global_active_power = global_active_power;
        this.global_reactive_power = global_reactive_power;
        this.voltage = voltage;
        this.global_intensity = global_intensity;
        this.sub_metering_1 = sub_metering_1;
        this.sub_metering_2 = sub_metering_2;
        this.sub_metering_3 = sub_metering_3;
        this.isInvalid = !valid;
        this.id = date + time;
    }

    public int hashCode() {
        return this.id.hashCode();
    }

    public String toString() {
        if (_string != null) return _string;
        StringBuilder sb = new StringBuilder();
        sb.append(date);
        sb.append(DELIMITER);
        sb.append(time);
        sb.append(DELIMITER);

        if (isInvalid) {
            sb.append("?;?;?;?;?;?;");
        } else {
            sb.append(String.format("%.3f", global_active_power));
            sb.append(DELIMITER);
            sb.append(String.format("%.3f", global_reactive_power));
            sb.append(DELIMITER);
            sb.append(String.format("%.3f", voltage));
            sb.append(DELIMITER);
            sb.append(String.format("%.3f", global_intensity));
            sb.append(DELIMITER);
            sb.append(String.format("%.3f", sub_metering_1));
            sb.append(DELIMITER);
            sb.append(String.format("%.3f", sub_metering_2));
            sb.append(DELIMITER);
            sb.append(String.format("%.3f", sub_metering_3));
        }

        _string = sb.toString();

        return _string;
    }
}
