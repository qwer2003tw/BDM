import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.FileOutputStream;
import java.util.List;
import java.util.regex.Pattern;

public final class Task1 {
    private static final Pattern COMMA = Pattern.compile(",");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("Task1")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> primaryTypeWords = lines.filter(s -> !s.contains("ID")).map(s -> COMMA.split(s)[5]);

        JavaRDD<String> locationDescriptionWords = lines.filter(s -> !s.contains("ID")).map(s -> COMMA.split(s)[7]);


        JavaPairRDD<String, Integer> primaryTypeOnes = primaryTypeWords.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> locationDescriptionOnes = locationDescriptionWords.mapToPair(s -> new Tuple2<>(s, 1));



        JavaPairRDD<String, Integer> primaryTypeCounts = primaryTypeOnes.reduceByKey((i1, i2) -> i1 + i2);

        JavaPairRDD<String, Integer> locationDescriptionCounts = locationDescriptionOnes.reduceByKey((i1, i2) -> i1 + i2);


        primaryTypeCounts = primaryTypeCounts.mapToPair(Tuple2::swap).sortByKey(false).mapToPair(Tuple2::swap);

        locationDescriptionCounts = locationDescriptionCounts.mapToPair(Tuple2::swap).sortByKey(false).mapToPair(Tuple2::swap);


        List<Tuple2<String, Integer>> primaryTypeOutput = primaryTypeCounts.collect();

        List<Tuple2<String, Integer>> locationDescriptionOutput = locationDescriptionCounts.collect();


        FileOutputStream outputStream = new FileOutputStream(args[1], false);

        outputStream.write("Task1:Primary type\n<value>, <count>\n".getBytes());
        for (Tuple2<?, ?> tuple : primaryTypeOutput) {
            String str = tuple._1() + ", " + tuple._2() + "\n";
            byte[] strToBytes = str.getBytes();
            outputStream.write(strToBytes);
        }

        outputStream.write("\n\nTask1:Location description\n<value>, <count>\n".getBytes());
        for (Tuple2<?, ?> tuple : locationDescriptionOutput) {
            String str = tuple._1() + ", " + tuple._2() + "\n";
            byte[] strToBytes = str.getBytes();
            outputStream.write(strToBytes);
        }
        outputStream.close();


        spark.stop();
    }
}