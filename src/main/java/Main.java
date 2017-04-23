import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.*;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) throws IOException {
        final String data_path = Utils.path;

        System.out.println("Data path: " + data_path);

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

        /*frequent columns
        4,failure,24471617
        14,smart_5_raw,24471593         Read Channel Margin
        58,smart_197_raw,24471593       Current Pending Sector Count
        60,smart_198_raw,24471593       Uncorrectable Sector Count
        50,smart_193_raw,24157811       Load/Unload Cycle Count
        */

        String[] v=("2016-01-01,WD-WCC4MKDL77ZK,WDC WD20EFRX,2000398934016,1,200,0,,,100,0,100,4,200,0,100,0,,,88,8807," +
                "100,0,100,0,100,4,,,,,,,,,,,,,,,,,,,,,200,2,200,571,120,27,,,200,0,200,0,100,0,200,0,100,0,,,,,,,,,,,,,," +
                ",,,,,,,,,,,,,,,,\n").split(",");

        final int[] colonneValide = new int[]{4, 14, 50, 58, 60};

        JavaRDD<String> textFileLastDay = spark_context.textFile(data_path + "AnalisiFrequenzaValori/lastDay.csv", 10000);

        JavaPairRDD<String, ArrayList<String>> healthy = textFileLastDay.mapToPair(riga ->
        {
            String key = "0";
            String[] valori = riga.split(",");
            ArrayList<String> lista = new ArrayList<>();
            if (valori[4].compareTo("0") == 0) {
                for (int i = 0; i < colonneValide.length; i++)
                    lista.add(valori[colonneValide[i]]);
            } else {
                key = "-1";
                for (int i = 0; i < colonneValide.length; i++)
                    lista.add("0");
            }
            return new Tuple2(key, lista);
        });

        JavaPairRDD<String, String> textFile = spark_context.wholeTextFiles(data_path + "Data", 1000);

        JavaPairRDD<String, ArrayList<Tuple2<String, ArrayList<String>>>> failedGroupByDay = textFile.mapToPair(file ->
        {
            String[] dischi = file._2().split(String.format("\n"));
            ArrayList<Tuple2<String, ArrayList<String>>> lista = new ArrayList<>();
            for (String disco : dischi) {
                String[] valori = disco.split(",");
                if (valori[4].compareTo("1") == 0) {
                    ArrayList<String> recordDisco = new ArrayList<>();

                    for (int i = 0; i < colonneValide.length; i++) {
                        if (valori[colonneValide[i]].compareTo("") == 0)
                            recordDisco.add("0");
                        recordDisco.add(valori[colonneValide[i]]);
                    }
                    lista.add(new Tuple2("1", recordDisco));
                }
            }
            return new Tuple2(1, lista);
        });

        JavaPairRDD<String, ArrayList<String>> failed = failedGroupByDay.flatMapToPair((PairFlatMapFunction<Tuple2<String, ArrayList<Tuple2<String, ArrayList<String>>>>, String, ArrayList<String>>) t -> {
            List<Tuple2<String, ArrayList<String>>> result = new ArrayList<>();

            for (Tuple2<String, ArrayList<String>> lista : t._2()) {
                result.add(new Tuple2(lista._1(), lista._2()));
            }
            return result.iterator();
        });

        List<Tuple2<String, ArrayList<String>>> failedCollected = failed.collect();
        List<Tuple2<String, ArrayList<String>>> healthyCollected = healthy.collect();

        String filename = data_path + "forDraw.csv";
        File fileOutput = new File(filename);
        if (!fileOutput.exists()) {
            fileOutput.createNewFile();
        }

        FileWriter fw = new FileWriter(fileOutput.getAbsoluteFile(), false); // creating fileWriter object with the file
        BufferedWriter bw = new BufferedWriter(fw); // creating bufferWriter which is used to write the content into the file

        String[] COLONNE=new String[]{"failure","Read Channel Margin",
                "Current Pending Sector Count", "Uncorrectable Sector Count",
                "Load_Unload Cycle Count"};
        for(String nome:COLONNE)
            bw.write(nome + ",");

        for (Tuple2<String, ArrayList<String>> record : failedCollected) {
            bw.write(record._1() + ",");
            for (String valore : record._2()) {
                bw.write(valore + ",");
            }
            bw.write(String.format("%n"));
        }

        for (Tuple2<String, ArrayList<String>> record : healthyCollected) {
            bw.write(record._1() + ",");
            for (String valore : record._2()) {
                bw.write(valore + ",");
            }
            bw.write(String.format("%n"));
        }
        bw.close();

        /*JavaPairRDD<String,ArrayList<String>> failed=textFile.flatMapToPair(new PairFlatMapFunction<Tuple2<String, ArrayList<String>>>(){
            public Iterable<Tuple2<String, ArrayList<String>>> call(ArrayList<String> list) {
                List<Tuple2<String, ArrayList<String>>> result = new ArrayList<>();
                result.add(new Tuple2<String, ArrayList<String>>("1", list));
                return result;
            }
            });*/
       /* JavaPairRDD<String, ArrayList<Tuple2<String, ArrayList<String>>>> failedGroupByDay = textFile.mapToPair(file ->
        {
            String[] dischi = file._2().split(String.format("\n"));
            ArrayList<Tuple2<String, ArrayList<String>>> lista = new ArrayList<>();
            for (String disco : dischi) {
                String[] valori = disco.split(",");
                if (valori[4].compareTo("1") == 0) {
                    ArrayList<String> recordDisco = new ArrayList<>();

                    for (int i = 0; i < colonneValide.length; i++)
                        recordDisco.add(valori[colonneValide[i]]);

                    lista.add(new Tuple2("1", recordDisco));
                }
            }
            return new Tuple2(1, lista);
        });*/

        /*
        JavaPairRDD<String, ArrayList<String>> failed = failedGroupByDay.flatMapToPair((Tuple2<String, ArrayList<String>> day) ->
        {
            ArrayList<String> tempList=day._2();
            return new Tuple2(day._1(),tempList);
        });
*/
/*
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
                valori[valori.length - 1] = "";
                valori[valori.length - 2] = "";
                for (int j = 0; j < colonneValide.length; j++) {
                    //scrive solo le colonne con indice contenuto in "colonneValide"
                    bw.write(valori[colonneValide[j]] + ",");
                }
                bw.write(String.format("%n"));
            }
            bw.close();
            return new Tuple2(file._1(), "DONE!");
        });

        JavaRDD<String> textFileLastDay = spark_context.textFile(data_path + "AnalisiFrequenzaValori/lastDay.csv", 10000);

        JavaPairRDD<String, ArrayList<String>> healthy = textFileLastDay.mapToPair(riga ->
        {
            String key = "0";
            String[] valori = riga.split(",");
            ArrayList<String> lista = new ArrayList<>();
            if (riga.contains("serial_number")) {
                key = "-1";
                lista.add("0");
                lista.add("0");
                lista.add("0");
            } else {
                for (int i = 0; i < colonneValide.length; i++)
                    lista.add(valori[colonneValide[i]]);
            }
            return new Tuple2(key, lista);
        });


        List<Tuple2<String, ArrayList<String>>> failedCollect = failed.collect();
        List<Tuple2<String, ArrayList<String>>> healthyCollect = healthy.collect();

        String filename = data_path + "forDraw.csv";
        File fileOutput = new File(filename);
        if (!fileOutput.exists()) {
            fileOutput.createNewFile();
        }
        bw.write("C" + ","+"X" + ","+"Y" + ","+"Z");

        FileWriter fw = new FileWriter(fileOutput.getAbsoluteFile(), false); // creating fileWriter object with the file
        BufferedWriter bw = new BufferedWriter(fw); // creating bufferWriter which is used to write the content into the file
        
        bw.write("C" + ","+"X" + ","+"Y" + ","+"Z");
        
        for (Tuple2<String, ArrayList<String>> record : healthyCollect) {
            bw.write(record._1() + ",");
            for (String valore : record._2()) {
                bw.write(valore + ",");
            }
            bw.write(String.format("%n"));
        }

        for (Tuple2<String, ArrayList<String>> record : failedCollect) {
            bw.write(record._1() + ",");
            for (String valore : record._2()) {
                bw.write(valore + ",");
            }
            bw.write(String.format("%n"));
        }
        bw.close();*/
    }
}