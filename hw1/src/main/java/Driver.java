import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

    public static void main(String[] args) throws Exception {

    /*
        args = new String[] { "hdfs://master:9000/user/hadoop/input1/",

                "hdfs://master:9000/user/hadoop/output/" };
    */

        /* delete the output directory before running the job */


        /* set the hadoop system parameter */

        //System.setProperty("hadoop.home.dir", "Replace this string with hadoop home directory location");

        if (args.length != 2) {
            System.err.println("Please specify the input and output path");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Driver.class);
        job.setJobName("Find_Max_Min_Avg");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        CheckAndDelete.checkAndDelete(args[1],conf);
        job.setMapperClass(EmployeeMinMaxCountMapper.class);
        job.setReducerClass(EmployeeMinMaxCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CustomMinMaxTuple.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
