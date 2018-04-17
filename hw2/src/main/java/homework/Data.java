package homework;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Data implements Serializable {
    public final int idLink;
    public final String title;
    public final String headline;
    public final String source;
    public final String topic;
    public final String publishDate;
    public final float sentimentTitle;
    public final float sentimentHeadline;
    public final int facebook;
    public final int googlePlus;
    public final int linkedIn;
    public final boolean isInvalid;
    private final static String DELIMITER = "\",\"";
    private String _string = null;

    Data(
            int idLink, String title, String headline, String source, String topic
            , String publishDate, float sentimentTitle
            , float sentimentHeadline, int facebook
            , int googlePlus, int linkedIn, boolean isInvalid
    ) {
        this.idLink = idLink;
        this.title = title;
        this.headline = headline;
        this.source = source;
        this.topic = topic;
        this.publishDate = publishDate;
        this.sentimentTitle = sentimentTitle;
        this.sentimentHeadline = sentimentHeadline;
        this.facebook = facebook;
        this.googlePlus = googlePlus;
        this.linkedIn = linkedIn;
        this.isInvalid = isInvalid;
    }

    private String[] getSplitArr(String data) {
        try {
            String[] dataArr = new String[0];
            InputStreamReader r = new InputStreamReader(new ByteArrayInputStream(data.getBytes()));
            CSVParser pr = CSVFormat.DEFAULT.parse(r);
            CSVRecord record = pr.getRecords().get(0);
            List<String> list = new ArrayList<>();
            record.iterator().forEachRemaining(list::add);
            dataArr = list.toArray(dataArr);
            return dataArr;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Data(String data) {
        String[] dataArr = getSplitArr(data);

        int idLink = 0;
        String title;
        String headline;
        String source;
        String topic;
        String publishDate;
        float sentimentTitle;
        float sentimentHeadline;
        int facebook = 0;
        int googlePlus = 0;
        int linkedIn = 0;
        boolean valid = false;
        try {
            if (dataArr.length == 11) {
                int idx = 0;
                idLink = Integer.valueOf(dataArr[idx++]);
                title = dataArr[idx++];
                headline = dataArr[idx++];
                source = dataArr[idx++];
                topic = dataArr[idx++];
                publishDate = dataArr[idx++];
                sentimentTitle = Float.valueOf(dataArr[idx++]);
                sentimentHeadline = Float.valueOf(dataArr[idx++]);
                facebook = Integer.valueOf(dataArr[idx++]);
                googlePlus = Integer.valueOf(dataArr[idx++]);
                linkedIn = Integer.valueOf(dataArr[idx++]);
                valid = true;
            } else {
                throw new Exception("Data error");
            }
        } catch (Exception e) {
            title = "N/A";
            headline = "N/A";
            source = "N/A";
            topic = "N/A";
            publishDate = "N/A";
            sentimentTitle = sentimentHeadline = -1;
        }

        this.idLink = idLink;
        this.title = title;
        this.headline = headline;
        this.source = source;
        this.topic = topic;
        this.publishDate = publishDate;
        this.sentimentTitle = sentimentTitle;
        this.sentimentHeadline = sentimentHeadline;
        this.facebook = facebook;
        this.googlePlus = googlePlus;
        this.linkedIn = linkedIn;
        this.isInvalid = !valid;
    }


}
