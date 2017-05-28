package Varie;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class ValoriOgniColonna {
    public static void ValoriOgniColonna(JavaSparkContext spark_context,String path)
    {
        final String data_path = path;
        System.out.println("Data path: " + data_path);

        JavaPairRDD<String, String> textFile = spark_context.wholeTextFiles(data_path + "Data", 20);

        JavaPairRDD<String, long[]> rows = textFile.mapToPair(file ->
        {
            String[] righe = file._2().split(String.format("%n"));
            long[] contatoreValori = new long[righe[0].split(",").length];

            for (int i = 0; i < contatoreValori.length; i++) {
                contatoreValori[i] = 0;
            }

            for (int i = 1; i < righe.length; i++) {
                String[] valori = righe[i].split(",");
                //forget the last column: smart_255_raw
                for (int j = 0; j < valori.length-1; j++) {
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

        String[] text=("date,serial_number,model,capacity_bytes,failure,smart_1_normalized,smart_1_raw," +
                "smart_2_normalized,smart_2_raw,smart_3_normalized,smart_3_raw,smart_4_normalized,smart_4_raw," +
                "smart_5_normalized,smart_5_raw,smart_7_normalized,smart_7_raw,smart_8_normalized,smart_8_raw," +
                "smart_9_normalized,smart_9_raw,smart_10_normalized,smart_10_raw,smart_11_normalized," +
                "smart_11_raw,smart_12_normalized,smart_12_raw,smart_13_normalized,smart_13_raw," +
                "smart_15_normalized,smart_15_raw,smart_22_normalized,smart_22_raw,smart_183_normalized," +
                "smart_183_raw,smart_184_normalized,smart_184_raw,smart_187_normalized,smart_187_raw," +
                "smart_188_normalized,smart_188_raw,smart_189_normalized,smart_189_raw,smart_190_normalized," +
                "smart_190_raw,smart_191_normalized,smart_191_raw,smart_192_normalized,smart_192_raw," +
                "smart_193_normalized,smart_193_raw,smart_194_normalized,smart_194_raw,smart_195_normalized," +
                "smart_195_raw,smart_196_normalized,smart_196_raw,smart_197_normalized,smart_197_raw," +
                "smart_198_normalized,smart_198_raw,smart_199_normalized,smart_199_raw,smart_200_normalized," +
                "smart_200_raw,smart_201_normalized,smart_201_raw,smart_220_normalized,smart_220_raw," +
                "smart_222_normalized,smart_222_raw,smart_223_normalized,smart_223_raw,smart_224_normalized," +
                "smart_224_raw,smart_225_normalized,smart_225_raw,smart_226_normalized,smart_226_raw," +
                "smart_240_normalized,smart_240_raw,smart_241_normalized,smart_241_raw,smart_242_normalized," +
                "smart_242_raw,smart_250_normalized,smart_250_raw,smart_251_normalized,smart_251_raw," +
                "smart_252_normalized,smart_252_raw,smart_254_normalized,smart_254_raw,smart_255_normalized," +
                "smart_255_raw").split(",");

        for (Tuple2<String, Long> elemento : output) {
            System.out.println(elemento._1() + ","+text[Integer.parseInt(elemento._1())]+"," + elemento._2());
        }
    }
}
