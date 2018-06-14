import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.FileOutputStream;
import java.util.*;
import java.util.regex.Pattern;

public final class Task2 {
    private static final Pattern COMMA = Pattern.compile(",");

    private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> unsortMap) {

        List<Map.Entry<K, V>> list =
                new LinkedList<>(unsortMap.entrySet());

        list.sort((o1, o2) -> -(o1.getValue()).compareTo(o2.getValue()));

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;

    }


    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("Task2")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();


        JavaPairRDD<String, Map<String, Integer>> pairs = lines
                .filter(s -> !s.contains("ID"))
                .mapToPair(s -> {
                    String[] words = COMMA.split(s);
                    return new Tuple2<>(words[7], new Tuple2<>(words[5], 1));
                })
                .groupByKey()
                .mapValues(iters -> {
                    Map<String, Integer> wordcount = new HashMap<>();
                    for (Tuple2<String, Integer> iter : iters) {
                        if (wordcount.keySet().contains(iter._1()))
                            wordcount.put(iter._1(), wordcount.get(iter._1()) + 1);
                        else wordcount.put(iter._1(), 1);
                    }
                    return sortByValue(wordcount);
                });


        List<Tuple2<String, Map<String, Integer>>> output = pairs.collect();


        FileOutputStream outputStream = new FileOutputStream(args[1], false);

        outputStream.write("Task2:\n\n\n".getBytes());
        for (Tuple2<String, Map<String, Integer>> tuple : output) {
            String str = tuple._1() + "\n\n";
            byte[] strToBytes = str.getBytes();
            outputStream.write(strToBytes);
            for (Map.Entry<String, Integer> entry : tuple._2().entrySet()) {
                outputStream.write((entry.getKey() + " " + entry.getValue()+"\n").getBytes());
            }
            outputStream.write(("\n").getBytes());
        }

        outputStream.close();


        spark.stop();
    }
}