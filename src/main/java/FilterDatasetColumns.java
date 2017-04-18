/*import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.hadoop.conf.Configuration;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException {
        //frequent columns
        final int[] colonneValide = new int[]{0, 1, 2, 3, 4, 94, 5, 6, 9, 10, 11, 12, 13, 14, 15, 16, 19, 20,
                21, 22, 25, 26, 57, 58, 59, 60, 61, 62, 51, 52, 47, 48};

        final String data_path = Utils.path;
        System.out.println("Data path: " + data_path);

        JavaSparkContext spark_context = new JavaSparkContext(new SparkConf()
                .setAppName("Spark Count")
                .setMaster("local")
        );

        JavaPairRDD<String, String> textFile = spark_context.wholeTextFiles(data_path + "Data", 1000);

        JavaPairRDD<String, String> rows = textFile.mapToPair(file ->
        {
            String[] filename = file._1().split("/");

            String fullFilename = data_path + "filteredColumns/" + filename[filename.length - 1];
            File fileFiltered = new File(fullFilename);
            if (!fileFiltered.exists()) {
                fileFiltered.createNewFile();
            }
            FileWriter fw = new FileWriter(fileFiltered.getAbsoluteFile(), false);
            BufferedWriter bw = new BufferedWriter(fw);

            String[] righe = file._2().split(String.format("\n"));
            for (int i = 0; i < righe.length; i++) {
                String[] valori = righe[i].split(",");

                //clean the last two character to fix a strange behaviour with new "line type of operations"
                //Gaspa remembers the mysterious "^M" on vim, unknow to Java (and to himself also).
                valori[valori.length-1]="";
                valori[valori.length-2]="";
                for (int j = 0; j < colonneValide.length; j++) {
                    //scrive solo le colonne con indice contenuto in "colonneValide"
                    bw.write(valori[colonneValide[j]] + ",");
                }
                bw.write(String.format("%n"));
            }
            bw.close();
            return new Tuple2(file._1(), "DONE!");
        });

        rows.foreach((Tuple2<String, String> tupla) -> System.out.println(tupla._1() + " " + tupla._2()));
    }
}
*/