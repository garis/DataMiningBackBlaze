import Varie.ContaDischiFalliti;
import Varie.Statistiche;
import Varie.ValoriOgniColonna;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException {

        final String data_path = Utils.path;

        File list[] = new File(data_path+"Data").listFiles();
        Arrays.sort(list);
        Path source = Paths.get(list[list.length-1].toString());
        Path destination = Paths.get(data_path+"lastDay.csv");

        Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);

        JavaSparkContext spark_context = new JavaSparkContext(new SparkConf()
                .setAppName("Spark Count")
                .setMaster("local")
        );

        //risolve "filesystem errors" se si usano i .jar
        //spark_context.hadoopConfiguration().set("fs.hdfs.impl",
        //        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        //);
        //spark_context.hadoopConfiguration().set("fs.file.impl",
        //        org.apache.hadoop.fs.LocalFileSystem.class.getName()
        //);

        //region CREA failedDisks.csv se non c'è

        String filename = data_path + "failedDisks.csv";
        File fileOutput = new File(filename);
        if (!fileOutput.exists()) {
            fileOutput.createNewFile();
            //carica il file .csv di ogni giorno
            JavaPairRDD<String, String> textFile = spark_context.wholeTextFiles(data_path + "Data", 400);

            /*per ogni giorno estrai tutti i dischi falliti in una lista di tuple, usando una tupla contenente una lista di tuple, una per ogni disco rotto in quel giorno
            esempio:
            Giorno 1 --> Tuple2(0,List)
                                |
                                |----Tuple2(1,(column_0,column_1,...))
                                |----Tuple2(1,(column_0,column_1,...))
                                |----Tuple2(1,(column_0,column_1,...))
            */
            JavaPairRDD<String, ArrayList<Tuple2<String, String>>> failedGroupByDay = textFile.mapToPair(file ->
            {
                String key = "-1";
                String[] dischi = file._2().split(String.format("%n"));
                ArrayList<Tuple2<String, ArrayList<String>>> lista = new ArrayList<>();
                for (String disco : dischi) {
                    key = "0";
                    String[] valori = disco.split(",");
                    if (valori.length >4 && valori[4].compareTo("1") == 0) {
                        lista.add(new Tuple2("1", disco));
                    }
                }
                return new Tuple2(key, lista);
            });

            /*ora tiriamo fuori ogni tupla dentro la lista di tuple
            Da questo:
            Day 1 --> Tuple2(0,List)
                                |
                                |----Tuple2(1,(column_0,column_1,...))
                                |----Tuple2(1,(column_0,column_1,...))
                                |----Tuple2(1,(column_0,column_1,...))
            A questo:
            Tuple2(1,(column_0,column_1,...))
            Tuple2(1,(column_0,column_1,...))
            Tuple2(1,(column_0,column_1,...))
            */
            JavaPairRDD<String, String> failedDisks = failedGroupByDay.flatMapToPair((PairFlatMapFunction<Tuple2<String, ArrayList<Tuple2<String, String>>>, String, String>) t -> {
                List<Tuple2<String, String>> resultFailed = new ArrayList<>();

                for (Tuple2<String, String> lista : t._2()) {
                    resultFailed.add(new Tuple2(lista._1(), lista._2()));
                }
                return resultFailed.iterator();
            });

            //e salviamo il risultato sul file failedDisks.csv
            List<Tuple2<String, String>> resultFailed = failedDisks.collect();

            FileWriter fw = new FileWriter(fileOutput.getAbsoluteFile(), false);
            BufferedWriter bw = new BufferedWriter(fw);

            for (Tuple2<String, String> tupla : resultFailed) {
                if (tupla._1().compareTo("-1") != 0) {
                    bw.write(tupla._2());
                    bw.write(String.format("%n"));
                }
            }
            bw.write(String.format("%n"));
            bw.close();
        }
        //endregion


        //decomentare ciò che si vuole fare

        //TrovaSoglie.TrovaSoglie(spark_context,Utils.path);
        MiningItemsets.MiningItemsets(spark_context,Utils.path);
        //AndamentoItemsets.AndamentoItemsets(spark_context,Utils.path,30);
        //ValoriOgniColonna.ValoriOgniColonna(spark_context,Utils.path);
        //Statistiche.StatisticheTemperatura(spark_context,Utils.path);
        //ContaDischiFalliti.ContaDischiFalliti(spark_context,Utils.path);
    }
}