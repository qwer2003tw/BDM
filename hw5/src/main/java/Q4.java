import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.FileOutputStream;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class Q4 {
    private static final Pattern TAB = Pattern.compile("\t");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        int iter = 20;

        if (args.length == 3) iter = Integer.valueOf(args[2]);

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaPairRDD<String, List<String>> pairsLink = lines
                .filter(s -> !s.contains(" "))
                .mapToPair(s -> {
                    String[] words = TAB.split(s);
                    return new Tuple2<>(words[0], words[1]);
                })
                .groupByKey().mapValues(Lists::newArrayList);

        JavaPairRDD<String, Double> pairsRank;

        pairsRank = lines
                .filter(s -> !s.contains(" "))
                .mapToPair(s -> new Tuple2<>(TAB.split(s)[0], 1.0))
                .distinct();

        JavaPairRDD<String, Double> contrib;

        for (int i = 0; i < iter; i++) {

            contrib = pairsLink
                    .join(pairsRank)
                    .values()
                    .flatMapToPair(s -> {
                        double size = s._1().size();
                        List<String> urls = s._1();
                        return urls.stream().map(url -> new Tuple2<>(url, s._2() / size)).collect(Collectors.toList()).iterator();
                    });

            pairsRank = contrib.reduceByKey((i1, i2) -> i1 + i2).mapValues(s -> s * .85 + .15);
        }

        pairsRank = pairsRank.mapToPair(Tuple2::swap).sortByKey(false).mapToPair(Tuple2::swap);

        List<Tuple2<String, Double>> output = pairsRank.collect();


        FileOutputStream outputStream = new FileOutputStream(args[1], false);

        outputStream.write("Q4:\n<NodeID>, <Rank>\n".getBytes());
        for (Tuple2<?, ?> tuple : output) {
            String str = tuple._1() + ", " + tuple._2() + "\n";
            byte[] strToBytes = str.getBytes();
            outputStream.write(strToBytes);
        }
        outputStream.close();


        spark.stop();
    }
}