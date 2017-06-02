package Varie;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ContaDischiFalliti {

    /**
     * Scrive in @param spark_context/failureStat.csv il numero di dischi falliti in ogni giorno
     * @param spark_context JavaSparkContext
     * @param path dataset path as String
     */
    public static void ContaDischiFalliti(JavaSparkContext spark_context,String path) throws IOException {
        final String data_path = path;
        System.out.println("Data path: " + data_path);

        JavaPairRDD<String, String> textFiles = spark_context.wholeTextFiles(data_path + "Data", 5);

        JavaPairRDD<String, Integer> fallimentiAlGiorno = textFiles.mapToPair(file ->
        {
            String[] righe = file._2().split(String.format("%n"));
            int contatore = 0;
            String data = "";
            //start from the second line
            for (int i = 1; i < righe.length; i++) {
                String[] valori = righe[i].split(",");

                data = valori[0];
                if (valori[4].compareTo("1") == 0) {
                    contatore++;
                }
            }
            return new Tuple2(data, contatore);
        });

        java.util.Map<String, Integer> risultato = fallimentiAlGiorno.collectAsMap();

        String filename = data_path + "failureStat.csv";
        File file = new File(filename);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        FileWriter fw = new FileWriter(file.getAbsoluteFile(), false);
        BufferedWriter bw = new BufferedWriter(fw);

        risultato.forEach((String key, Integer value) -> {
            try {
                bw.write(key + "," + value + String.format("%n"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        bw.close();
    }
}
