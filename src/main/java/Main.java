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

import javax.xml.bind.SchemaOutputResolver;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Main {

    public static void main(String[] args) throws IOException {
        final long startTime = System.currentTimeMillis();
        final String[] valuesAR = new String[]{"R_ERR", "SPIN-UP", "S&S", "REALLOC", "HOURS", "SPIN_ERR", "POWER-CYCL","RETRACT", "LOAD&UNL", "UNST-SEC","ABS-ERR"};
        final int[] columnIndex = new int[]{6, 10, 12, 14, 20, 22, 26, 48, 50, 58, 60};
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

            JavaPairRDD<String, String> textFile = spark_context.wholeTextFiles(data_path + "Data", 400);

            JavaPairRDD<String, ArrayList<Tuple2<String, String>>> failedGroupByDay = textFile.mapToPair(file ->
            {
                String key = "-1";
                String[] dischi = file._2().split(String.format("%n"));
                ArrayList<Tuple2<String, ArrayList<String>>> lista = new ArrayList<>();
                for (String disco : dischi) {
                    key = "0";
                    String[] valori = disco.split(",");
                    if (valori.length > 4 && valori[4].compareTo("1") == 0) {
                        lista.add(new Tuple2("1", disco));
                    }
                }
                return new Tuple2(key, lista);
            });

            JavaPairRDD<String, String> failedDisks = failedGroupByDay.flatMapToPair((PairFlatMapFunction<Tuple2<String, ArrayList<Tuple2<String, String>>>, String, String>) t -> {
                List<Tuple2<String, String>> resultFailed = new ArrayList<>();

                for (Tuple2<String, String> lista : t._2()) {
                    resultFailed.add(new Tuple2(lista._1(), lista._2()));
                }
                return resultFailed.iterator();
            });

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
        // Cluster the data into two classes using KMeans
        int NUMBERCLUSTER = 5;
        int numIterations = 20;
        long[][] centriInt = new long[columnIndex.length][NUMBERCLUSTER];

        for (int i = 0; i < columnIndex.length; i++)
        {
            final int INDICECOLONNA = columnIndex[i];

            //same thing as above
            JavaRDD<String> data = spark_context.textFile(data_path + "AnalisiFrequenzaValori/failedDisks.csv", 1);

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

            KMeansModel clusters = KMeans.train(parsedData.rdd(), NUMBERCLUSTER, numIterations);

            System.out.println("Cluster centers (column: " + INDICECOLONNA + "):");
            int o=0;
            for (Vector center : clusters.clusterCenters()) {
                double[] vettore = center.toArray();
                System.out.printf("%d  ", Math.round(vettore[0]));
                centriInt[i][o++] = Math.round(vettore[0]);
                // /System.out.printf("%f", String.format("%.0f", Double.parseDouble( center.toString())));//);BigDecimal.valueOf(Double.parseDouble(center.toString())));
            }
        }

        long[][] soglieInt = new long[columnIndex.length][NUMBERCLUSTER-1]; //Thresholds array
        for(int i = 0; i<columnIndex.length; i++)
        {
            List<Long> temp = new ArrayList<Long>(); //Temporary array used to sort centers
            for(int o = 0; o<NUMBERCLUSTER; o++)
            {
                temp.add(centriInt[i][o]);
            }
            Collections.sort(temp);
            for(int o = 1; o<NUMBERCLUSTER; o++)
            {
                soglieInt[i][o-1] = (temp.get(o-1)+temp.get(o))/2;
                System.out.printf("%d  ", soglieInt[i][o-1]);
            }
            System.out.print("\n");
        }

        //Printing strings (kinda) ready to be copy-pasted, just delete the last comma
        //WARNING: F'ing gross & messy, like, Filthy Frank's hair cake gross.
        //Names
        final int mode=0; //Change -1 to 0 to remove the first threshold (from 0 to x)
        final String finalString = "Double.MAX_VALUE";
        System.out.print("{");
        for(int i = 0; i<columnIndex.length; i++)
        {
            for (int o = mode; o < NUMBERCLUSTER-1; o++)
            {
                System.out.print("\"" + valuesAR[i] + "_" + ((int)(o+1)) + "\",\t\t");
            }
        }
        System.out.println("}");
        //Thresholds (lower)
        System.out.print("{");
        for(int i = 0; i<columnIndex.length; i++)
        {
            for (int o = mode; o < NUMBERCLUSTER-1; o++)
            {
                if (o < 0)
                    System.out.print("0,\t\t");
                else
                    System.out.print(soglieInt[i][o] + ",\t\t");
            }
        }
        System.out.println("}");
        //Thresholds (Higher)
        System.out.print("{");
        for(int i = 0; i<columnIndex.length; i++)
        {
            for (int o = mode+1; o <= NUMBERCLUSTER-1; o++)
            {
                if (o == (NUMBERCLUSTER - 1))
                    System.out.print(finalString + ",\t\t");
                else
                    System.out.print(soglieInt[i][o] + ",\t\t");
            }
        }
        System.out.println("}");
        //Column Indexes
        System.out.print("{");
        for(int i = 0; i<columnIndex.length; i++)
        {
            for (int o = mode+1; o <= NUMBERCLUSTER-1; o++)
            {
                System.out.print(columnIndex[i] + ",\t\t");
            }
        }
        System.out.println("}");
        final long endTime = System.currentTimeMillis();
        System.out.println("Total execution time: " + (endTime - startTime));
    }
}