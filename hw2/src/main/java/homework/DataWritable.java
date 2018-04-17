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
        int idLink = in.readInt();
        String title = in.readUTF();
        String headline = in.readUTF();
        String source = in.readUTF();
        String topic = in.readUTF();
        String publishDate = in.readUTF();
        float sentimentTitle = in.readFloat();
        float sentimentHeadline = in.readFloat();
        int facebook = in.readInt();
        int googlePlus = in.readInt();
        int linkedIn = in.readInt();
        boolean isInvalid = in.readBoolean();

        this.value = new Data(idLink, title, headline, source, topic, publishDate, sentimentTitle, sentimentHeadline, facebook, googlePlus, linkedIn, isInvalid);
    }


    public void write(DataOutput out) throws IOException {
        out.writeInt(this.value.idLink);
        out.writeUTF(this.value.title);
        out.writeUTF(this.value.headline);
        out.writeUTF(this.value.source);
        out.writeUTF(this.value.topic);
        out.writeUTF(this.value.publishDate);
        out.writeFloat(this.value.sentimentTitle);
        out.writeFloat(this.value.sentimentHeadline);
        out.writeFloat(this.value.facebook);
        out.writeFloat(this.value.googlePlus);
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

}
