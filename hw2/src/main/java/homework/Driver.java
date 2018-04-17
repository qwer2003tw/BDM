package homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;


import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.DoubleStream;


public class Driver {
    final static Logger logger = Logger.getLogger(Driver.class);

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, FloatWritable> {

        private Text id;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String line = value.toString();
            if (line.startsWith("\"IDLink\",\"Title\",")) return;

            DataWritable data = new DataWritable(line);
            if (data.get().isInvalid) return;

            FloatWritable sentimentHeadline = new FloatWritable(data.get().sentimentHeadline);
            FloatWritable sentimentTitle = new FloatWritable(data.get().sentimentTitle);
            FloatWritable sentimentTotal = new FloatWritable(sentimentHeadline.get() + sentimentTitle.get());
            id = new Text(data.get().topic);
            context.write(id, sentimentTotal);
            id = new Text("sentimentHeadline-" + data.get().topic);
            context.write(id, sentimentHeadline);
            id = new Text("sentimentTitle-" + data.get().topic);
            context.write(id, sentimentTitle);
        }
    }


    public static class DataReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            float reduceSum = 0f;
            for (FloatWritable val : values) {
                reduceSum += val.get();
            }
            result.set(reduceSum);
            context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("mapred-site.xml");
        conf.addResource("yarn-site.xml");
        Job job = Job.getInstance(conf, "Homework1");
        job.setJarByClass(Driver.class);

        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(DataCombiner.class);
        job.setReducerClass(DataReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        CheckAndDelete.checkAndDelete(args[1], conf);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
