import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException {
        final String data_path = Utils.path;
        final int columnTemperature = 29;

        System.out.println("Data path: " + data_path);

        JavaSparkContext spark_context = new JavaSparkContext(new SparkConf()
                .setAppName("Spark Count")
                .setMaster("local")
                //uffa
                .set("spark.executor.memory", "2g")
        );

        //fix filesystem errors when using java .jar execution
        spark_context.hadoopConfiguration().set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        spark_context.hadoopConfiguration().set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );

        JavaPairRDD<String, String> textFile = spark_context.wholeTextFiles(data_path + "filteredColumns/", 10000);

        JavaPairRDD<String, ArrayList<Double>> temperature = textFile.mapToPair(fileDisco ->
        {
            String[] tempKey = fileDisco._1().split("/");
            tempKey[0] = tempKey[tempKey.length - 1];//reuse the value at 0 as name

            ArrayList<Double> dischi = new ArrayList<Double>();
            for (String giorno : fileDisco._2().split(String.format("%n"))) {
                if (!giorno.contains("date")) {
                    try {
                        dischi.add(Double.parseDouble(giorno.split(",")[columnTemperature]));
                    } catch (Exception ex) {//sometimes the temperature is missing :(
                        dischi.add(25D);
                    }
                }
            }
            return new Tuple2(tempKey[0], dischi);
        });

        temperature.cache();
        JavaPairRDD<String, ArrayList<Double>> result = temperature.mapToPair((Tuple2<String, ArrayList<Double>> temperatureGiornata) ->
        {
            double mean = 0;
            double sum = 0;
            for (Double temperatura : temperatureGiornata._2())
                sum = sum + temperatura.intValue();

            mean = sum / (double) temperatureGiornata._2().size();

            double DevSTD = 0;
            for (Double temperatura : temperatureGiornata._2())
                DevSTD = Math.pow(temperatura.doubleValue() - mean, 2);

            DevSTD = DevSTD / (double) temperatureGiornata._2().size();
            DevSTD = Math.sqrt(DevSTD);

            ArrayList<Double> valori = new ArrayList<>();
            valori.add(mean);
            valori.add(DevSTD);
            return new Tuple2<String, ArrayList<Double>>(temperatureGiornata._1(), valori);
        });

        JavaPairRDD<String, String> output = result.mapToPair((Tuple2<String, ArrayList<Double>> valori) ->
                new Tuple2<String, String>("0", valori._1() +
                        "," + valori._2().get(0).doubleValue() +
                        "," + valori._2().get(1).doubleValue())
        );

        output.reduceByKey((String StrA, String StrB) -> {
            return StrA + String.format("%n") + StrB;
                }
        ).foreach((Tuple2<String, String> allInOne) -> System.out.print(allInOne._2()));
    }
}