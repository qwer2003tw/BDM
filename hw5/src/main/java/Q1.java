import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.FileOutputStream;
import java.util.List;
import java.util.regex.Pattern;

public final class Q1 {
    private static final Pattern TAB = Pattern.compile("\t");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines.filter(s -> !s.contains(" ")).map(s -> TAB.split(s)[0]);

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));


        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);


        counts = counts.mapToPair(Tuple2::swap).sortByKey(false).mapToPair(Tuple2::swap);


        List<Tuple2<String, Integer>> output = counts.collect();


        FileOutputStream outputStream = new FileOutputStream(args[1], false);

        outputStream.write("Q1:\n<NodeID>, <out-degree>\n".getBytes());
        for (Tuple2<?, ?> tuple : output) {
            String str = tuple._1() + ", " + tuple._2() + "\n";
            byte[] strToBytes = str.getBytes();
            outputStream.write(strToBytes);
        }
        outputStream.close();


        spark.stop();
    }
}