package homework;


import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Public
@Stable
public class DataWritable implements Writable /*WritableComparable<DataWritable>*/ {
    private Data value;

    DataWritable() {
    }

    DataWritable(String line) {
        set(line);
    }

    public void set(String line) {
        this.value = new Data(line);
    }

    public Data get() {
        return this.value;
    }

    public void readFields(DataInput in) throws IOException {
        String id = in.readUTF();
        String date = in.readUTF();
        String time = in.readUTF();
        float global_active_power = in.readFloat();
        float global_reactive_power = in.readFloat();
        float voltage = in.readFloat();
        float global_intensity = in.readFloat();
        float sub_metering_1 = in.readFloat();
        float sub_metering_2 = in.readFloat();
        float sub_metering_3 = in.readFloat();
        boolean isInvalid = in.readBoolean();

        this.value = new Data(id, date, time, global_active_power, global_reactive_power, voltage, global_intensity, sub_metering_1, sub_metering_2, sub_metering_3, isInvalid);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.value.id);
        out.writeUTF(this.value.date);
        out.writeUTF(this.value.time);
        out.writeFloat(this.value.global_active_power);
        out.writeFloat(this.value.global_reactive_power);
        out.writeFloat(this.value.voltage);
        out.writeFloat(this.value.global_intensity);
        out.writeFloat(this.value.sub_metering_1);
        out.writeFloat(this.value.sub_metering_2);
        out.writeFloat(this.value.sub_metering_3);
        out.writeBoolean(this.value.isInvalid);
    }

    public boolean equals(Object o) {
        if (!(o instanceof DataWritable)) {
            return false;
        } else {
            DataWritable other = (DataWritable) o;
            return this.value == other.value;
        }
    }

    public int hashCode() {
        return this.value.hashCode();
    }


/*
    @Override
    public int compareTo(DataWritable o) {
        return this.value.id.compareTo(o.value.id);
    }

    static {
        WritableComparator.define(DataWritable.class, new DataWritable.Comparator());
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(DataWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int thisValue = readInt(b1, s1);
            int thatValue = readInt(b2, s2);
            return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
        }
    }*/
}
