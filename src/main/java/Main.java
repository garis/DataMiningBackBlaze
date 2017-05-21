import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import scala.Tuple2;

import javax.xml.bind.SchemaOutputResolver;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException {
        final long startTime = System.currentTimeMillis();
        /*!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        HELP:
        sistemare la classe Utils mettendo il path corretto del dataset
        creare la cartella (dentro al path di Utils) "AnalisiFrequenzaValori"
        copiare dentro ad essa il file "2016-12-31.csv" del dataset
        e tutto dovrebbe andare (ammess odi aver configurato Hadoop e Spark)
        !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!*/

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
6,smart_1_raw,24471593      R_ERR:      Read Error Rate
10,smart_3_raw,24471593     SPIN-UP:    Spin-Up Time
12,smart_4_raw,24471593     S&S:        Start/Stop Count
14,smart_5_raw,24471593     REALLOC:    Reallocated Sectors Count
16,smart_7_raw,24471593     SEEK-ERR:   Seek Error Rate
20,smart_9_raw,24471593     HOURS:      Power-On Hours
22,smart_10_raw,24471593    SPIN-ERR:   Spin Retry Count
26,smart_12_raw,24471593    POWER-CYCL: Power Cycle Count
58,smart_197_raw,24471593   UNST-SEC:   Current Pending Sector Count
60,smart_198_raw,24471593   ABS-ERR:    Uncorrectable Sector Count
62,smart_199_raw,24471593   IF-ERR:     UltraDMA CRC Error Count
52,smart_194_raw,24471342   TEMP:       Temperature
48,smart_192_raw,24428958   RETRACT:    Number of power-off or emergency retract cycles
50,smart_193_raw,24157811   LOAD&UNL:   Count of load/unload cycles into head landing zone position
         */


        // @formatter:off

        //example of Pastea's idea implementations (thresholds with overlapping boundaries) with example values:    0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16  17  18  19  20  21  22 ............................
        //final String[] valuesAR = new String[]{      "R_ERR_LOW", "R_ERR_MED", "R_ERR_HIGH"};                     |-------------------R_ERR_LOW--------------------|                      |--------------------R_ERR_HIGH-------..........
        //final double[] lowerThreshold = new double[]{       0,           8,           18};                                                              |-----------------------R_ERR_MED------------------------|
        //final double[] upperThreshold = new double[]{ 12,          22,          Double.MAX_VALUE};
        //final int[] columnIndex = new int[]{          6,           6,           6};
        //in word this means: for the sixth column we want three item:  one if the value >= 0 and <= 12
        //                                                              one if the value >= 8 and <= 22
        //                                                              one if the value >= 18 and <= +inf

        //this give a name to the item, very useful when reading the rules
        final String[] valuesAR = new String[]{"R_ERR_0",		"R_ERR_1",		"R_ERR_2",		"R_ERR_3",		"R_ERR_4",		"SPIN-UP_0",		"SPIN-UP_1",		"SPIN-UP_2",		"SPIN-UP_3",		"SPIN-UP_4",		"S&S_0",		"S&S_1",		"S&S_2",		"S&S_3",		"S&S_4",		"REALLOC_0",		"REALLOC_1",		"REALLOC_2",		"REALLOC_3",		"REALLOC_4",		"HOURS_0",		"HOURS_1",		"HOURS_2",		"HOURS_3",		"HOURS_4",		"SPIN_ERR_0",		"SPIN_ERR_1",		"SPIN_ERR_2",		"SPIN_ERR_3",		"SPIN_ERR_4",		"POWER-CYCL_0",		"POWER-CYCL_1",		"POWER-CYCL_2",		"POWER-CYCL_3",		"POWER-CYCL_4",		"RETRACT_0",		"RETRACT_1",		"RETRACT_2",		"RETRACT_3",		"RETRACT_4",		"LOAD&UNL_0",		"LOAD&UNL_1",		"LOAD&UNL_2",		"LOAD&UNL_3",		"LOAD&UNL_4",		"UNST-SEC_0",		"UNST-SEC_1",		"UNST-SEC_2",		"UNST-SEC_3",		"UNST-SEC_4",		"ABS-ERR_0",		"ABS-ERR_1",		"ABS-ERR_2",		"ABS-ERR_3",		"ABS-ERR_4"};
        final double[] lowerThreshold = new double[]{0,		111631401,		665349234,		2161913152L,		3523312379L,		0,		2077,		4248,		5664,		7568,		0,		28,		80,		572,		9401,		0,		1676,		8583,		24423,		48796,		0,		7452,		16354,		26375,		39619,		0,		4,		131078,		262150,		327685,		0,		17,		51,		174,		663,		0,		646,		6710,		22851,		49391,		0,		134124,		718022,		2911961,		5225821,		0,		670,		3980,		11585,		30764,		0,		1542,		6482,		11906,		29444};
        final double[] upperThreshold = new double[]{111631401,		665349234,		2161913152L,		3523312379L,		Double.MAX_VALUE,		2077,		4248,		5664,		7568,		Double.MAX_VALUE,		28,		80,		572,		9401,		Double.MAX_VALUE,		1676,		8583,		24423,		48796,		Double.MAX_VALUE,		7452,		16354,		26375,		39619,		Double.MAX_VALUE,		4,		131078,		262150,		327685,		Double.MAX_VALUE,		17,		51,		174,		663,		Double.MAX_VALUE,		646,		6710,		22851,		49391,		Double.MAX_VALUE,		134124,		718022,		2911961,		5225821,		Double.MAX_VALUE,		670,		3980,		11585,		30764,		Double.MAX_VALUE,		1542,		6482,		11906,		29444,		Double.MAX_VALUE};
        final int[] columnIndex = new int[]{6,		6,		6,		6,		6,		10,		10,		10,		10,		10,		12,		12,		12,		12,		12,		14,		14,		14,		14,		14,		20,		20,		20,		20,		20,		22,		22,		22,		22,		22,		26,		26,		26,		26,		26,		48,		48,		48,		48,		48,		50,		50,		50,		50,		50,		58,		58,		58,		58,		58,		60,		60,		60,		60,		60};
        //columnIndex is the list of interesting columns
        // @formatter:on

        double TOTALE_DISCHI_ROTTI=0,TOTALE_DISCHI_SANI=0;
        System.out.println(valuesAR.length + " " + lowerThreshold.length + " " + upperThreshold.length + " " + columnIndex.length);
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

        //region CREA failedDisks.csv se non c'è

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

        //endregion

        //region DISCHI SANI

        //load the file
        JavaRDD<String> textFileLastDay = spark_context.textFile(data_path + "AnalisiFrequenzaValori/lastDay.csv", 1);

        //for each line make the tuple
        //each tuple rappresent a disk
        //but we load only the columns that we want based on the columnIndex, lowerThreshold and valuesAR
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
                        //filtra tutti i valori = 0 perchè dopo una breve discussione abbiamo deciso che un valore a zero non
                        //dovrebbe essere presente nell'analisi
                        if (value > 0 && value >= lowerThreshold[i] && value <= upperThreshold[i])
                            lista.add(valuesAR[i]);
                    }
                }
            } else
                key = "-1";
            return new Tuple2(key, lista);
        });

        //and now we buffer the result (the filtered files) in a file on disk
        //this is a bit useless because we do not gain a lot of perfomance but is great for debug
        List<Tuple2<String, ArrayList<String>>> result = healthy.collect();

        TOTALE_DISCHI_SANI=result.size();

        filename = data_path + "forFrequentPatternMiningHEALTHY.txt";
        fileOutput = new File(filename);
        if (!fileOutput.exists()) {
            fileOutput.createNewFile();
        }

        FileWriter fw = new FileWriter(fileOutput.getAbsoluteFile(), false);
        BufferedWriter bw = new BufferedWriter(fw);

        for (Tuple2<String, ArrayList<String>> tupla : result) {
            if (tupla._1().compareTo("-1") != 0) {
                for (String lettera : tupla._2())
                    bw.write(lettera + " ");
                bw.write(String.format("%n"));
            }
        }
        bw.write(String.format("%n"));
        bw.close();

        //endregion

        //region DISCHI ROTTI

        //same thing as above
        JavaRDD<String> textFileFailed = spark_context.textFile(data_path + "AnalisiFrequenzaValori/failedDisks.csv", 1);

        JavaPairRDD<String, ArrayList<String>> failed = textFileFailed.mapToPair(riga ->
        {
            String key = "0";
            String[] valori = riga.split(",");
            ArrayList<String> lista = new ArrayList<>();
            //if failed check
            //N.B: verifies that there are at least some valid values because some records have small number of values (like 6)
            if (valori.length > 6 && valori[4].compareTo("1") == 0) {
                for (int i = 0; i < columnIndex.length; i++) {
                    if (!valori[columnIndex[i]].isEmpty()) {
                        double value = Double.parseDouble(valori[columnIndex[i]]);
                        //filtra tutti i valori = 0 perchè dopo una breve discussione abbiamo deciso che un valore a zero non
                        //dovrebbe essere presente nell'analisi
                        if (value > 0 && value >= lowerThreshold[i] && value <= upperThreshold[i])
                            lista.add(valuesAR[i]);
                    }
                }
            } else
                key = "-1";
            return new Tuple2(key, lista);
        });

        List<Tuple2<String, ArrayList<String>>> resultF = failed.collect();
        TOTALE_DISCHI_ROTTI=resultF.size();

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

        //endregion

        //region MINING DISCHI ROTTI
        //really not much to say
        //pretty much copy and paste from the documentation

        JavaRDD<String> data = spark_context.textFile(data_path + "forFrequentPatternMiningFAILED.txt");

        JavaRDD<List<String>> transactions = data.map(line -> Arrays.asList(line.split(" ")));

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(0.4)
                .setNumPartitions(10);
        FPGrowthModel<String> model = fpg.run(transactions);

        filename = data_path + "resultFrequentPatternMiningFAILED.csv";
        fileOutput = new File(filename);
        if (!fileOutput.exists()) {
            fileOutput.createNewFile();
        }

        fw = new FileWriter(fileOutput.getAbsoluteFile(), false); // creating fileWriter object with the file
        bw = new BufferedWriter(fw); // creating bufferWriter which is used to write the content into the file

        for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect()) {
            bw.write("[" + itemset.javaItems() + "], " + ((double)(itemset.freq())/TOTALE_DISCHI_ROTTI)+String.format("%n"));
        }
        bw.close();

        //endregion

        //region MINING DISCHI SANI
        data = spark_context.textFile(data_path + "forFrequentPatternMiningHEALTHY.txt");

        transactions = data.map(line -> Arrays.asList(line.split(" ")));

        fpg = new FPGrowth()
                .setMinSupport(0.4)
                .setNumPartitions(10);
        model = fpg.run(transactions);

        filename = data_path + "resultFrequentPatternMiningHEALTHY.csv";
        fileOutput = new File(filename);
        if (!fileOutput.exists()) {
            fileOutput.createNewFile();
        }

        fw = new FileWriter(fileOutput.getAbsoluteFile(), false); // creating fileWriter object with the file
        bw = new BufferedWriter(fw); // creating bufferWriter which is used to write the content into the file

        for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect()) {
            bw.write("[" + itemset.javaItems() + "], " + ((double)(itemset.freq())/TOTALE_DISCHI_SANI)+String.format("%n"));
        }

        bw.close();

        //endregion

        final long endTime = System.currentTimeMillis();
        System.out.println("Total execution time: " + (endTime - startTime));
    }
}