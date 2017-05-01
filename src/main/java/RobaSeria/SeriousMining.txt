import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException {
        /*From this (see:"ContaQuantiValoriInOgniColonna.txt"):
0,date,24471617
1,serial_number,24471617
2,model,24471617
3,capacity_bytes,24471617
4,failure,24471617
5,smart_1_normalized,24471593
6,smart_1_raw,24471593
9,smart_3_normalized,24471593
10,smart_3_raw,24471593
11,smart_4_normalized,24471593
12,smart_4_raw,24471593
13,smart_5_normalized,24471593
14,smart_5_raw,24471593
15,smart_7_normalized,24471593
16,smart_7_raw,24471593
19,smart_9_normalized,24471593
20,smart_9_raw,24471593
21,smart_10_normalized,24471593
22,smart_10_raw,24471593
25,smart_12_normalized,24471593
26,smart_12_raw,24471593
57,smart_197_normalized,24471593
58,smart_197_raw,24471593
59,smart_198_normalized,24471593
60,smart_198_raw,24471593
61,smart_199_normalized,24471593
62,smart_199_raw,24471593
51,smart_194_normalized,24471342
52,smart_194_raw,24471342
47,smart_192_normalized,24428958
48,smart_192_raw,24428958
49,smart_193_normalized,24157811
50,smart_193_raw,24157811

we ignore the normalized values, date, SN, model and capacity. We obtain:
4,failure,24471617
6,smart_1_raw,24471593
10,smart_3_raw,24471593
12,smart_4_raw,24471593
14,smart_5_raw,24471593
16,smart_7_raw,24471593
20,smart_9_raw,24471593
22,smart_10_raw,24471593
26,smart_12_raw,24471593
58,smart_197_raw,24471593
60,smart_198_raw,24471593
62,smart_199_raw,24471593
52,smart_194_raw,24471342
48,smart_192_raw,24428958
50,smart_193_raw,24157811
         */


        // @formatter:off
        final String[] valuesAR = new String[]{"A", "B", "C", "D", "E", "F", "G", "H", "I", "L", "M", "N", "O", "P", "Q"};
        final int[] lowerThreshold = new int[]{ 1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1};
        final int[] columnIndex = new int[]{    4,   6,   10,  12,  14,  16,  20,  22,  26,  58,  60,  62,  52,  48,  50};
        // @formatter:on
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

        //all failed disks on on file
        //if the file doesn't exist (or it is the first run of the code) make a new one

        String filename = data_path + "AnalisiFrequenzaValori/failedDisks.csv";
        File fileOutput = new File(filename);
        if (!fileOutput.exists()) {
            fileOutput.createNewFile();
            JavaPairRDD<String, String> textFile = spark_context.wholeTextFiles(data_path + "Data", 400);

            JavaPairRDD<String, ArrayList<Tuple2<String, String>>> failedGroupByDay = textFile.mapToPair(file ->
            {
                String key = "-1";
                String[] dischi = file._2().split(String.format("\n"));
                ArrayList<Tuple2<String, ArrayList<String>>> lista = new ArrayList<>();
                for (String disco : dischi) {
                    key = "0";
                    String[] valori = disco.split(",");
                    if (valori[4].compareTo("1") == 0) {
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

        //now the analisys

        //healthy disks

        JavaRDD<String> textFileLastDay = spark_context.textFile(data_path + "AnalisiFrequenzaValori/lastDay.csv", 1);

        JavaPairRDD<String, ArrayList<String>> healthy = textFileLastDay.mapToPair(riga ->
        {
            String key = "0";
            String[] valori = riga.split(",");
            ArrayList<String> lista = new ArrayList<>();
            //if not failed
            if (valori[4].compareTo("0") == 0) {
                for (int i = 0; i < columnIndex.length; i++) {
                    if (!valori[columnIndex[i]].isEmpty()) {
                        double value = Double.parseDouble(valori[columnIndex[i]]);
                        if (value > lowerThreshold[i])
                            lista.add(valuesAR[i]);
                    }
                }
            } else
                key = "-1";
            return new Tuple2(key, lista);
        });

        List<Tuple2<String, ArrayList<String>>> result = healthy.collect();

        filename = data_path + "forFrequentPatternMiningHEALTHY.txt";
        fileOutput = new File(filename);
        if (!fileOutput.exists()) {
            fileOutput.createNewFile();
        }

        FileWriter fw = new FileWriter(fileOutput.getAbsoluteFile(), false); // creating fileWriter object with the file
        BufferedWriter bw = new BufferedWriter(fw); // creating bufferWriter which is used to write the content into the file

        for (Tuple2<String, ArrayList<String>> tupla : result) {
            if (tupla._1().compareTo("-1") != 0) {
                for (String lettera : tupla._2())
                    bw.write(lettera + " ");
                bw.write(String.format("%n"));
            }
        }
        bw.write(String.format("%n"));
        bw.close();

        //failed disks

        JavaRDD<String> textFileFailed = spark_context.textFile(data_path + "AnalisiFrequenzaValori/failedDisks.csv", 1);

        JavaPairRDD<String, ArrayList<String>> failed = textFileFailed.mapToPair(riga ->
        {
            String key = "0";
            String[] valori = riga.split(",");
            ArrayList<String> lista = new ArrayList<>();
            //if failed check
            //N.B: valori.length >= columnIndex.length is a very bad check but is sufficient to verify that there are at least some valid values
            //because some records have small number of values (like 6)
            if (valori.length > 6 && valori[4].compareTo("1") == 0) {

                for (int i = 0; i < columnIndex.length; i++) {
                    if (!valori[columnIndex[i]].isEmpty()) {
                        double value = Double.parseDouble(valori[columnIndex[i]]);
                        if (value > lowerThreshold[i])
                            lista.add(valuesAR[i]);
                    }
                }
            } else
                key = "-1";
            return new Tuple2(key, lista);
        });

        List<Tuple2<String, ArrayList<String>>> resultF = failed.collect();

        filename = data_path + "forFrequentPatternMiningFAILED.txt";
        fileOutput = new File(filename);
        if (!fileOutput.exists()) {
            fileOutput.createNewFile();
        }

        fw = new FileWriter(fileOutput.getAbsoluteFile(), false); // creating fileWriter object with the file
        bw = new BufferedWriter(fw); // creating bufferWriter which is used to write the content into the file

        for (Tuple2<String, ArrayList<String>> tupla : resultF) {
            if (tupla._1().compareTo("-1") != 0) {
                for (String lettera : tupla._2())
                    bw.write(lettera + " ");
                bw.write(String.format("%n"));
            }
        }
        bw.write(String.format("%n"));
        bw.close();


        //mining

        //JavaRDD<String> data = spark_context.textFile(data_path+"forFrequentPatternMiningHEALTHY.txt");
        JavaRDD<String> data = spark_context.textFile(data_path+"forFrequentPatternMiningFAILED.txt");

        JavaRDD<List<String>> transactions = data.map(line -> Arrays.asList(line.split(" ")));
        long total=transactions.count();

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(0.4)
                .setNumPartitions(10);
        FPGrowthModel<String> model = fpg.run(transactions);

        for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
            //System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
        }

        double minConfidence = 0.8;

        filename = data_path + "resultFrequentPatternMiningFAILED.csv";
        fileOutput = new File(filename);
        if (!fileOutput.exists()) {
            fileOutput.createNewFile();
        }

        fw = new FileWriter(fileOutput.getAbsoluteFile(), false); // creating fileWriter object with the file
        bw = new BufferedWriter(fw); // creating bufferWriter which is used to write the content into the file

        for (AssociationRules.Rule<String> rule
                : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
            bw.write(rule.javaAntecedent() + ", " + rule.javaConsequent() + ", " + rule.confidence()+", "+(rule.freqUnion()/total)+String.format("%n"));
        }
        bw.close();

        data = spark_context.textFile(data_path+"forFrequentPatternMiningHEALTHY.txt");

        transactions = data.map(line -> Arrays.asList(line.split(" ")));
        total=transactions.count();

        fpg = new FPGrowth()
                .setMinSupport(0.4)
                .setNumPartitions(10);
        model = fpg.run(transactions);

        for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
            //System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
        }

        filename = data_path + "resultFrequentPatternMiningHEALTHY.csv";
        fileOutput = new File(filename);
        if (!fileOutput.exists()) {
            fileOutput.createNewFile();
        }

        fw = new FileWriter(fileOutput.getAbsoluteFile(), false); // creating fileWriter object with the file
        bw = new BufferedWriter(fw); // creating bufferWriter which is used to write the content into the file

        for (AssociationRules.Rule<String> rule
                : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
            bw.write(rule.javaAntecedent() + ", " + rule.javaConsequent() + ", " + rule.confidence()+", "+(rule.freqUnion()/total)+String.format("%n"));
        }
        bw.close();
    }
}