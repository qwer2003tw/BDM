package homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.DoubleStream;


public class Driver {
    final static Logger logger = Logger.getLogger(Driver.class);

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, DataWritable> {

        private Text id = new Text("qwert");

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String line = value.toString();
            if (line.startsWith("Date;Time;")) return;
            DataWritable data = new DataWritable(line);
            if (data.get().isInvalid) return;

            context.write(id, data);
        }
    }

    private static double getSd(DoubleStream doubleStream, double mean, int count) {
        double powerSum = doubleStream.map(x -> x * x).sum();
        double x = powerSum - mean * mean * count;

        return Math.sqrt(x / (count - 1));
    }

    private static float getNormValue(double value, double min, double diff) {
        if (diff == 0) return 0;
        return (float) ((value - min) / diff);
    }

    public static class DataReducer extends Reducer<Text, DataWritable, Text, Text> {
        private static final Text key_count = new Text("count");
        private static final Text key_mean_global_active_power = new Text("mean_global_active_power");
        private static final Text key_mean_global_reactive_power = new Text("mean_global_reactive_power");
        private static final Text key_mean_global_intensity = new Text("mean_global_intensity");
        private static final Text key_mean_voltage = new Text("mean_voltage");


        private static final Text key_sd_global_active_power = new Text("sd_global_active_power");
        private static final Text key_sd_global_reactive_power = new Text("sd_global_reactive_power");
        private static final Text key_sd_global_intensity = new Text("sd_global_intensity");
        private static final Text key_sd_voltage = new Text("sd_voltage");


        private static final Text key_min_global_active_power = new Text("min_global_active_power");
        private static final Text key_min_global_reactive_power = new Text("min_global_reactive_power");
        private static final Text key_min_global_intensity = new Text("min_global_intensity");
        private static final Text key_min_voltage = new Text("min_voltage");

        private static final Text key_max_global_active_power = new Text("max_global_active_power");
        private static final Text key_max_global_reactive_power = new Text("max_global_reactive_power");
        private static final Text key_max_global_intensity = new Text("max_global_intensity");
        private static final Text key_max_voltage = new Text("max_voltage");

        private static final Text key_norm_data = new Text("norm_data");

        public void reduce(Text key, Iterable<DataWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            ArrayList<Data> dataSet = new ArrayList<>();
            for (DataWritable val : values) {
                dataSet.add(val.get());
            }
            dataSet = dataSet.parallelStream().filter(x -> !x.isInvalid).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
            int count = dataSet.size();

            double sum_global_active_power = dataSet.parallelStream().mapToDouble(x -> x.global_active_power).sum();
            double sum_global_reactive_power = dataSet.parallelStream().mapToDouble(x -> x.global_reactive_power).sum();
            double sum_global_intensity = dataSet.parallelStream().mapToDouble(x -> x.global_intensity).sum();
            double sum_voltage = dataSet.parallelStream().mapToDouble(x -> x.voltage).sum();

            double mean_global_active_power = sum_global_active_power / count;
            double mean_global_reactive_power = sum_global_reactive_power / count;
            double mean_global_intensity = sum_global_intensity / count;
            double mean_voltage = sum_voltage / count;


            double sd_global_active_power = getSd(dataSet.parallelStream().mapToDouble(x -> x.global_active_power), mean_global_active_power, count);
            double sd_global_reactive_power = getSd(dataSet.parallelStream().mapToDouble(x -> x.global_reactive_power), mean_global_reactive_power, count);
            double sd_global_intensity = getSd(dataSet.parallelStream().mapToDouble(x -> x.global_intensity), mean_global_intensity, count);
            double sd_voltage = getSd(dataSet.parallelStream().mapToDouble(x -> x.voltage), mean_global_intensity, count);


            double min_global_active_power = dataSet.parallelStream().mapToDouble(x -> x.global_active_power).min().orElse(-1);
            double min_global_reactive_power = dataSet.parallelStream().mapToDouble(x -> x.global_reactive_power).min().orElse(-1);
            double min_global_intensity = dataSet.parallelStream().mapToDouble(x -> x.global_intensity).min().orElse(-1);
            double min_voltage = dataSet.parallelStream().mapToDouble(x -> x.voltage).min().orElse(-1);
            double min_sub_metering_1 = dataSet.parallelStream().mapToDouble(x -> x.sub_metering_1).min().orElse(-1);
            double min_sub_metering_2 = dataSet.parallelStream().mapToDouble(x -> x.sub_metering_2).min().orElse(-1);
            double min_sub_metering_3 = dataSet.parallelStream().mapToDouble(x -> x.sub_metering_3).min().orElse(-1);

            double max_global_active_power = dataSet.parallelStream().mapToDouble(x -> x.global_active_power).max().orElse(-1);
            double max_global_reactive_power = dataSet.parallelStream().mapToDouble(x -> x.global_reactive_power).max().orElse(-1);
            double max_global_intensity = dataSet.parallelStream().mapToDouble(x -> x.global_intensity).max().orElse(-1);
            double max_voltage = dataSet.parallelStream().mapToDouble(x -> x.voltage).max().orElse(-1);
            double max_sub_metering_1 = dataSet.parallelStream().mapToDouble(x -> x.sub_metering_1).max().orElse(-1);
            double max_sub_metering_2 = dataSet.parallelStream().mapToDouble(x -> x.sub_metering_2).max().orElse(-1);
            double max_sub_metering_3 = dataSet.parallelStream().mapToDouble(x -> x.sub_metering_3).max().orElse(-1);


            context.write(key_count, new Text(String.valueOf(count)));

            context.write(key_mean_global_active_power, new Text(String.valueOf(mean_global_active_power)));
            context.write(key_mean_global_reactive_power, new Text(String.valueOf(mean_global_reactive_power)));
            context.write(key_mean_global_intensity, new Text(String.valueOf(mean_global_intensity)));
            context.write(key_mean_voltage, new Text(String.valueOf(mean_voltage)));


            context.write(key_sd_global_active_power, new Text(String.valueOf(sd_global_active_power)));
            context.write(key_sd_global_reactive_power, new Text(String.valueOf(sd_global_reactive_power)));
            context.write(key_sd_global_intensity, new Text(String.valueOf(sd_global_intensity)));
            context.write(key_sd_voltage, new Text(String.valueOf(sd_voltage)));


            context.write(key_min_global_active_power, new Text(String.valueOf(min_global_active_power)));
            context.write(key_min_global_reactive_power, new Text(String.valueOf(min_global_reactive_power)));
            context.write(key_min_global_intensity, new Text(String.valueOf(min_global_intensity)));
            context.write(key_min_voltage, new Text(String.valueOf(min_voltage)));

            context.write(key_max_global_active_power, new Text(String.valueOf(max_global_active_power)));
            context.write(key_max_global_reactive_power, new Text(String.valueOf(max_global_reactive_power)));
            context.write(key_max_global_intensity, new Text(String.valueOf(max_global_intensity)));
            context.write(key_max_voltage, new Text(String.valueOf(max_voltage)));


            //Norm data

            {

                String[] lines = new String[count + 1];
                lines[0] = "Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;Sub_metering_1;Sub_metering_2;Sub_metering_3";
                double diff_global_active_power = max_global_active_power - min_global_active_power;
                double diff_global_reactive_power = max_global_reactive_power - min_global_reactive_power;
                double diff_global_intensity = max_global_intensity - min_global_intensity;
                double diff_voltag = max_voltage - min_voltage;
                double diff_sub_metering_1 = max_sub_metering_1 - min_sub_metering_1;
                double diff_sub_metering_2 = max_sub_metering_2 - min_sub_metering_2;
                double diff_sub_metering_3 = max_sub_metering_3 - min_sub_metering_3;
                for (int i = dataSet.size() - 1; i >= 0; i--) {
                    final Data d = dataSet.get(i);
                    lines[i + 1] = new Data(
                            d.id, d.date, d.time,
                            getNormValue(d.global_active_power, min_global_active_power, diff_global_active_power),
                            getNormValue(d.global_reactive_power, min_global_reactive_power, diff_global_reactive_power),
                            getNormValue(d.global_intensity, min_global_intensity, diff_global_intensity),
                            getNormValue(d.voltage, min_voltage, diff_voltag),
                            getNormValue(d.sub_metering_1, min_sub_metering_1, diff_sub_metering_1),
                            getNormValue(d.sub_metering_2, min_sub_metering_2, diff_sub_metering_2),
                            getNormValue(d.sub_metering_3, min_sub_metering_3, diff_sub_metering_3),
                            d.isInvalid
                    ).toString();
                }
                OperatingFiles ofs = new OperatingFiles();
                String dir = "/user/hadoop";
                System.out.println("\n=======create a file=======");
                String fileContent = String.join("\n", lines);
                ofs.createFile(dir + "/normalization.csv", fileContent);
                //context.write(key_norm_data, new Text(String.join("\n", lines)));

            }

        }
    }

    /*
        public static class DataCombiner
                extends Reducer<Text, IntWritable, Text, IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(Text key, Iterable<IntWritable> values,
                               Context context
            ) throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                result.set(sum);
                context.write(key, result);
            }
        }

        public static class FloatSumCombiner
                extends Reducer<Text, FloatWritable, Text, FloatWithLengthWritable> {
            private FloatWithLengthWritable result = new FloatWithLengthWritable();

            public void reduce(Text key, Iterable<FloatWritable> values,
                               Context context
            ) throws IOException, InterruptedException {
                float sum = 0;
                int count = 0;
                for (FloatWritable val : values) {
                    sum += val.get();
                    count++;
                }
                result.setValue(sum);
                result.setLength(count);
                context.write(key, result);
            }
        }
    */
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
        job.setMapOutputValueClass(DataWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        CheckAndDelete.checkAndDelete(args[1], conf);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
