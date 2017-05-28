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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TrovaSoglie {
    public static void TrovaSoglie(JavaSparkContext spark_context) throws IOException {
        final long startTime = System.currentTimeMillis();
        final String[] valuesAR = new String[]{"R_ERR", "SPIN-UP", "S&S", "REALLOC", "HOURS", "SPIN_ERR", "POWER-CYCL", "RETRACT", "LOAD&UNL", "UNST-SEC", "ABS-ERR"};
        final int[] columnIndex = new int[]{6, 10, 12, 14, 20, 22, 26, 48, 50, 58, 60};
        final String data_path = Utils.path;

        System.out.println("Data path: " + data_path);

        //now we want all failed disks on on file
        //if the file doesn't exist (or it is the first run of the code) make a new one

        String filename = data_path + "failedDisks.csv";
        File fileOutput = new File(filename);

        //failed disks
        // Cluster the data into two classes using KMeans
        int NUMBERCLUSTER = 5;
        int numIterations = 20;
        long[][] centriInt = new long[columnIndex.length][NUMBERCLUSTER];

        for (int i = 0; i < columnIndex.length; i++) {
            final int INDICECOLONNA = columnIndex[i];

            //same thing as above
            JavaRDD<String> data = spark_context.textFile(data_path + "failedDisks.csv", 1);

            JavaRDD<Vector> parsedData = data.map(riga ->
            {
                String[] vettore = riga.split(",");
                double[] valore = new double[]{0};
                if (vettore.length >= INDICECOLONNA && !vettore[INDICECOLONNA].isEmpty())
                    valore[0] = Double.parseDouble(vettore[INDICECOLONNA]);
                else
                    valore[0] = 0;
                return Vectors.dense(valore);
            }).filter((Vector valore) ->
            {
                //filtra tutti i valori minori di zero
                if ((int) valore.toArray()[0] <= 0) return false;
                return true;
            });

            KMeansModel clusters = KMeans.train(parsedData.rdd(), NUMBERCLUSTER, numIterations);

            List<Long> tempSort = new ArrayList<>();
            for (Vector center : clusters.clusterCenters())
                tempSort.add(Math.round(center.toArray()[0]));
            Collections.sort(tempSort);
            final Long primaSoglia = (tempSort.get(0) + tempSort.get(1)) / 2;
            JavaRDD<Vector> parsedData2 = parsedData.filter((x) ->
            {
                if ((int) x.toArray()[0] <= primaSoglia) return true;
                return false;
            });

            KMeansModel clusters2 = KMeans.train(parsedData2.rdd(), NUMBERCLUSTER, numIterations);


            int o = 0;
            for (Vector center : clusters2.clusterCenters())
                centriInt[i][o++] = Math.round(center.toArray()[0]);

        }

        long[][] soglieInt = new long[columnIndex.length][NUMBERCLUSTER - 1]; //Thresholds array
        for (int i = 0; i < columnIndex.length; i++) {
            List<Long> temp = new ArrayList<Long>(); //Temporary array used to sort centers
            for (int o = 0; o < NUMBERCLUSTER; o++) {
                temp.add(centriInt[i][o]);
            }
            Collections.sort(temp);
            System.out.printf("%d   ", 1);
            for (int o = 1; o < NUMBERCLUSTER; o++) {
                soglieInt[i][o - 1] = (temp.get(o - 1) + temp.get(o)) / 2;
                System.out.printf("%d   ", soglieInt[i][o - 1]);
            }
            System.out.print("\n");
        }

        //Printing strings (kinda) ready to be copy-pasted, just delete the last comma
        //WARNING: F'ing gross & messy, like, Filthy Frank's hair cake gross.
        //Names
        final int mode = -1; //Change -1 to 0 to remove the first threshold (from 0 to x)
        final String finalString = "Double.MAX_VALUE";
        System.out.print("{");
        for (int i = 0; i < columnIndex.length; i++) {
            for (int o = mode; o < NUMBERCLUSTER - 1; o++) {
                System.out.print("\"" + valuesAR[i] + "_" + ((int) (o + 1)) + "\",\t\t");
            }
        }
        System.out.println("}");
        //Thresholds (lower)
        System.out.print("{");
        for (int i = 0; i < columnIndex.length; i++) {
            for (int o = mode; o < NUMBERCLUSTER - 1; o++) {
                if (o < 0)
                    System.out.print("0,\t\t");
                else
                    System.out.print(soglieInt[i][o] + ",\t\t");
            }
        }
        System.out.println("}");
        //Thresholds (Higher)
        System.out.print("{");
        for (int i = 0; i < columnIndex.length; i++) {
            for (int o = mode + 1; o <= NUMBERCLUSTER - 1; o++) {
                if (o == (NUMBERCLUSTER - 1))
                    System.out.print(finalString + ",\t\t");
                else
                    System.out.print(soglieInt[i][o] + ",\t\t");
            }
        }
        System.out.println("}");
        //Column Indexes
        System.out.print("{");
        for (int i = 0; i < columnIndex.length; i++) {
            for (int o = mode + 1; o <= NUMBERCLUSTER - 1; o++) {
                System.out.print(columnIndex[i] + ",\t\t");
            }
        }
        System.out.println("}");
        final long endTime = System.currentTimeMillis();
        System.out.println("Total execution time: " + (endTime - startTime));
    }
}
