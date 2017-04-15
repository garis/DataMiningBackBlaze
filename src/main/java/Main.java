import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException {
        final String data_path = Utils.path;
        System.out.println("Data path: " + data_path);

        JavaSparkContext spark_context = new JavaSparkContext(new SparkConf()
                .setAppName("Spark Count")
                .setMaster("local")
        );

        JavaPairRDD<String, String> textFile = spark_context.wholeTextFiles(data_path + "Data", 1000);

        JavaPairRDD<String, long[]> rows = textFile.mapToPair(file ->
        {
            String[] righe = file._2().split(String.format("%n"));
            long[] contatoreValori = new long[righe[0].split(",").length];

            for (int i = 0; i < contatoreValori.length; i++) {
                contatoreValori[i] = 0;
            }

            for (int i = 1; i < righe.length; i++) {
                String[] valori = righe[i].split(",");
                for (int j = 0; j < valori.length; j++) {
                    if (valori[j].compareTo("") != 0) {
                        contatoreValori[j]++;
                    }
                }
            }
            return new Tuple2("A", contatoreValori);
        });

        JavaPairRDD<String, long[]> rddMapReduce = rows.reduceByKey((long[] vettoreA, long[] vettoreB) ->
        {
            for (int h = 0; h < vettoreB.length; h++) {
                vettoreA[h] = vettoreA[h] + vettoreB[h];
            }
            return vettoreA;
        });
        //List<Tuple2<String, long[]>> fromMapReduceCollect=rddMapReduce.collect();
        Tuple2<String, long[]> colonneValuesCount = rddMapReduce.collect().get(0);

        ArrayList<Tuple2<String, Long>> output = new ArrayList<Tuple2<String, Long>>();
        for (int i = 0; i < colonneValuesCount._2().length; i++) {
            output.add(new Tuple2("" + i, colonneValuesCount._2()[i]));
        }

        Collections.sort(output, new Comparator<Tuple2<String, Long>>() {
            public int compare(Tuple2<String, Long> value, Tuple2<String, Long> otherValue) {
                return -(value._2().compareTo(otherValue._2()));//the minus is for descending order (max to min)
            }
        });

        for (Tuple2<String, Long> elemento : output) {
            System.out.println(elemento._1() + "  " + elemento._2());
        }
    }
}
