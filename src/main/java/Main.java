import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException {
        final long startTime = System.currentTimeMillis();

        final String data_path = Utils.path;

        System.out.println("Data path: " + data_path);

        //initialize the Spark context in Java
        JavaSparkContext spark_context = new JavaSparkContext(new SparkConf()
                .setAppName("Spark Count")
                .setMaster("local")
        );


        //fix filesystem errors when using java .jar execution
        spark_context.hadoopConfiguration().set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        spark_context.hadoopConfiguration().set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );

        //now we want all failed disks on on file
        //if the file doesn't exist (or it is the first run of the code) make a new one

        String filename = data_path + "AnalisiFrequenzaValori/failedDisks.csv";
        File fileOutput = new File(filename);
        if (!fileOutput.exists()) {
            fileOutput.createNewFile();
            //on all the dataset (so for each of the 366 days of the dataset) load the .csv file...
            JavaPairRDD<String, String> textFile = spark_context.wholeTextFiles(data_path + "Data", 400);

            //for each day (so for each .csv file) extract all the failed hard drive in a list of tuple, a tuple2 for each disk
            //example: Day 1: Tuple2(0,List(Tuple2(1,...),Tuple2(1,...),...)
            //
            //maybe the next diagram is a bit more clear:
            //Day 1 --> Tuple2(0,List)
            //                    |
            //                    |----Tuple2(1,(column_0,column_1,...))
            //                    |----Tuple2(1,(column_0,column_1,...))
            //                    |----Tuple2(1,(column_0,column_1,...))
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

            //now we want a tuple(key,value) for each failed hard drive
            //so we extract for every day all the failed disks and with that
            //we create a Tuple(key,values) for every disk
            //diagram:
            //from this:
            //Day 1 --> Tuple2(0,List)
            //                    |
            //                    |----Tuple2(1,(column_0,column_1,...))
            //                    |----Tuple2(1,(column_0,column_1,...))
            //                    |----Tuple2(1,(column_0,column_1,...))
            //to this:
            //Tuple2(1,(column_0,column_1,...))
            //Tuple2(1,(column_0,column_1,...))
            //Tuple2(1,(column_0,column_1,...))
            //
            //we extract the tuples from the tuple of each day
            JavaPairRDD<String, String> failedDisks = failedGroupByDay.flatMapToPair((PairFlatMapFunction<Tuple2<String, ArrayList<Tuple2<String, String>>>, String, String>) t -> {
                List<Tuple2<String, String>> resultFailed = new ArrayList<>();

                for (Tuple2<String, String> lista : t._2()) {
                    resultFailed.add(new Tuple2(lista._1(), lista._2()));
                }
                return resultFailed.iterator();
            });

            //for speed we write the result to a file so the next time we gain speed
            //because we don't need anymore to analyze the entire dataset
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

        //failed disks

        final int INDICECOLONNA=6;

        //same thing as above
        JavaRDD<String> data  = spark_context.textFile(data_path + "AnalisiFrequenzaValori/failedDisks.csv", 1);

        JavaRDD<Vector> parsedData = data.map(riga ->
        {
            String[] vettore =riga.split(",");
            double[] valore=new double[]{0};
            if(vettore.length>=6)
                valore[0]=Double.parseDouble(vettore[INDICECOLONNA]);
            return Vectors.dense(valore);
        }).filter((Vector valore)->
        {
            //filtra tutti i valori minori di zero
            if((int)valore.toArray()[0]<=0) return false;
            return true;
        });

        // Cluster the data into two classes using KMeans
        int NUMBERCLUSTER = 5;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), NUMBERCLUSTER, numIterations);

        System.out.println("Cluster centers:");
        for (Vector center: clusters.clusterCenters()) {
            double[] vettore=center.toArray();
            System.out.printf("%f%n", vettore[0]);
            // /System.out.printf("%f", String.format("%.0f", Double.parseDouble( center.toString())));//);BigDecimal.valueOf(Double.parseDouble(center.toString())));
        }

        final long endTime = System.currentTimeMillis();
        System.out.println("Total execution time: " + (endTime - startTime));
    }
}