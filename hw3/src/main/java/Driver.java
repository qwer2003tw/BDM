import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.URI;

public class Driver {
    public static Configuration conf = new Configuration();
    public static int documentTotal = 0;


    public static void DataHandle(Configuration conf, int n) throws Exception {
        FileSystem hdfs = FileSystem.get(new URI("hdfs://master:9000"), conf);
        String index = String.format("%02d", n);
        Path src = new Path("input_hw3/reut2-0" + index + ".sgm");
        Path dst = new Path("input/0" + index + ".txt");
        StringBuilder data = new StringBuilder();

        FSDataInputStream inputStream = hdfs.open(src);
        String content = IOUtils.toString(inputStream, "ISO-8859-1");
        inputStream.close();

        Document doc = Jsoup.parse(content, "ISO-8859-1", Parser.xmlParser());
        Elements bodys = doc.getElementsByTag("body");
        for (Element body : bodys) {
            data.append(body.text());
            data.append(System.getProperty("line.separator"));
        }

        FSDataOutputStream output = hdfs.create(dst);
        output.writeBytes(data.toString());
        output.close();
        hdfs.close();

    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text id;

        public void map(Object key, Text value, Context context) {
            final String line = value.toString();
            DataOutput da=new DataOutput(line,3);
        }
    }


    public static class DataReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {


        public void reduce(Text key, Iterable<FloatWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            float reduceSum = 0f;
            int count = 0;
            Text sumstr = new Text(key + "_Sum:");
            Text avgstr = new Text(key + "_Average:");
            for (FloatWritable val : values) {
                reduceSum += val.get();
                count++;
            }
            context.write(sumstr, new FloatWritable(reduceSum));
            context.write(avgstr, new FloatWritable(reduceSum / count));
        }
    }

    public static void main(String[] args) throws Exception {
        conf.addResource("core-site.xml");
        conf.addResource("mapred-site.xml");
        conf.addResource("yarn-site.xml");
        Job job = Job.getInstance(conf, "Homework3");
        job.setJarByClass(Driver.class);
//        for (int i = 0; i < 22; i++) {
//            DataHandle(conf, i);
//        }
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(DataCombiner.class);
        job.setReducerClass(DataReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        CheckAndDelete.checkAndDelete(args[1], conf);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
