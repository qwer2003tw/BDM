package homework;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FloatWithLengthWritable implements Writable {
    private float value;
    private int length;

    public void setValue(float value) {
        this.value = value;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public float getValue() {
        return this.value;
    }

    public int getLength() {
        return this.length;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(value);
        dataOutput.writeInt(length);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.value = dataInput.readFloat();
        this.length = dataInput.readInt();
    }
}