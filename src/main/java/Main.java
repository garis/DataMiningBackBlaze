import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

public class Main {

    public static void main(String[] args) {
        final String data_path = Utils.path;
        System.out.println("Data path: " + data_path);

        JavaSparkContext spark_context = new JavaSparkContext(new SparkConf()
                .setAppName("Spark Count")
                .setMaster("local")
        );

        JavaRDD<String> textFile = spark_context.textFile(data_path + "Out1_HardDisksFiles");

        JavaPairRDD<String, ArrayList<String>> rows = textFile.mapToPair((String riga) ->
        {
            String key = "";
            ArrayList<String> valoriSMART = new ArrayList<String>();
            if (riga.contains("serial_number")) {
                key = "0";
                valoriSMART.add("0");
            } else {
                String[] valori = riga.split(",");
                ///if failed
                if (valori[3].compareTo("1")==0) {
                    //key is the SN
                    key = "" + valori[1];
                    for (int i = 0; i < 4; i++) {
                        valoriSMART.add("" + valori[i]);
                    }
                }
                else {
                    key = "0";
                    valoriSMART.add("0");
                }
            }
            return new Tuple2(key, valoriSMART);
        });

        String filename = data_path + "failureStat.csv";
        File file = new File(filename);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        FileWriter fw = null; // creating fileWriter object with the file
        try {
            fw = new FileWriter(file.getAbsoluteFile(), false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter bw = new BufferedWriter(fw); // creating bufferWriter which is used to write the content into the file
        String value = "";

        int anno = 2016;
        for (int mese = 1; mese < 13; mese++) {
            for (int giorno = 1; giorno < 32; giorno++) {
                final String data = anno + "-" +String.format("%02d", mese) + "-" + String.format("%02d", giorno);
                java.util.Map<String,Long> number = rows.filter((Tuple2<String, ArrayList<String>> riga) ->
                {
                    if (riga._2().get(0).compareTo(data) == 0)
                        return true;
                    return false;
                }).countByKey();

                String line = data + ","+number.size()+"," +String.format("%n", "");
                try {
                    bw.write(line);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    bw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
