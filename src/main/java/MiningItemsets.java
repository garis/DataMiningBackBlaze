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

public class MiningItemsets {
    public void MiningItemsets(JavaSparkContext spark_context) throws IOException {
        final long startTime = System.currentTimeMillis();

        /*colonna X ha Y valori
0,date,48612972
1,serial_number,48612972
2,model,48612972
3,capacity_bytes,48612972
4,failure,48612972
5,smart_1_normalized,48612495
6,smart_1_raw,48612495
9,smart_3_normalized,48612495
10,smart_3_raw,48612495
11,smart_4_normalized,48612495
12,smart_4_raw,48612495
13,smart_5_normalized,48612495
14,smart_5_raw,48612495
15,smart_7_normalized,48612495
16,smart_7_raw,48612495
19,smart_9_normalized,48612495
20,smart_9_raw,48612495
21,smart_10_normalized,48612495
22,smart_10_raw,48612495
25,smart_12_normalized,48612495
26,smart_12_raw,48612495
57,smart_197_normalized,48612495
58,smart_197_raw,48612495
59,smart_198_normalized,48612495
60,smart_198_raw,48612495
61,smart_199_normalized,48612495
62,smart_199_raw,48612495
51,smart_194_normalized,48611881
52,smart_194_raw,48611881
47,smart_192_normalized,48094330
48,smart_192_raw,48094330
49,smart_193_normalized,47628884
50,smart_193_raw,47628884

ignoriamo valori normalizzati, date, SN, modello e capacità. Rimaniamo con:
4,failure,48612972
6,smart_1_raw,48612495      R_ERR:      Read Error Rate
10,smart_3_raw,48612495     SPIN-UP:    Spin-Up Time
12,smart_4_raw,48612495     S&S:        Start/Stop Count
14,smart_5_raw,48612495     REALLOC:    Reallocated Sectors Count
16,smart_7_raw,48612495     SEEK-ERR:   Seek Error Rate
20,smart_9_raw,48612495     HOURS:      Power-On Hours
22,smart_10_raw,48612495    SPIN-ERR:   Spin Retry Count
26,smart_12_raw,48612495    POWER-CYCL: Power Cycle Count
58,smart_197_raw,48612495   UNST-SEC:   Current Pending Sector Count
60,smart_198_raw,48612495   ABS-ERR:    Uncorrectable Sector Count
62,smart_199_raw,48612495   IF-ERR:     UltraDMA CRC Error Count
52,smart_194_raw,48611881   TEMP:       Temperature
48,smart_192_raw,48094330   RETRACT:    Number of power-off or emergency retract cycles
50,smart_193_raw,47628884   LOAD&UNL:   Count of load/unload cycles into head landing zone position
         */


        //dati estratti usando il codice in TrovaSoglie.java
        final String[] valuesAR = new String[]{"R_ERR_0",		"R_ERR_1",		"R_ERR_2",		"R_ERR_3",		"R_ERR_4",		"SPIN-UP_0",		"SPIN-UP_1",		"SPIN-UP_2",		"SPIN-UP_3",		"SPIN-UP_4",		"S&S_0",		"S&S_1",		"S&S_2",		"S&S_3",		"S&S_4",		"REALLOC_0",		"REALLOC_1",		"REALLOC_2",		"REALLOC_3",		"REALLOC_4",		"HOURS_0",		"HOURS_1",		"HOURS_2",		"HOURS_3",		"HOURS_4",		"SPIN_ERR_0",		"SPIN_ERR_1",		"SPIN_ERR_2",		"SPIN_ERR_3",		"SPIN_ERR_4",		"POWER-CYCL_0",		"POWER-CYCL_1",		"POWER-CYCL_2",		"POWER-CYCL_3",		"POWER-CYCL_4",		"RETRACT_0",		"RETRACT_1",		"RETRACT_2",		"RETRACT_3",		"RETRACT_4",		"LOAD&UNL_0",		"LOAD&UNL_1",		"LOAD&UNL_2",		"LOAD&UNL_3",		"LOAD&UNL_4",		"UNST-SEC_0",		"UNST-SEC_1",		"UNST-SEC_2",		"UNST-SEC_3",		"UNST-SEC_4",		"ABS-ERR_0",		"ABS-ERR_1",		"ABS-ERR_2",		"ABS-ERR_3",		"ABS-ERR_4"};
        final double[] lowerThreshold = new double[]{1,		111631401,		665349234,		2161913152L,		3523312379L,		1,		2077,		4248,		5664,		7568,		1,		28,		80,		572,		9401,		1,		1676,		8583,		24423,		48796,		1,		7452,		16354,		26375,		39619,		1,		4,		131078,		262150,		327685,		1,		17,		51,		174,		663,		1,		646,		6710,		22851,		49391,		1,		134124,		718022,		2911961,		5225821,		1,		670,		3980,		11585,		30764,		1,		1542,		6482,		11906,		29444};
        final double[] upperThreshold = new double[]{111631401,		665349234,		2161913152L,		3523312379L,		Double.MAX_VALUE,		2077,		4248,		5664,		7568,		Double.MAX_VALUE,		28,		80,		572,		9401,		Double.MAX_VALUE,		1676,		8583,		24423,		48796,		Double.MAX_VALUE,		7452,		16354,		26375,		39619,		Double.MAX_VALUE,		4,		131078,		262150,		327685,		Double.MAX_VALUE,		17,		51,		174,		663,		Double.MAX_VALUE,		646,		6710,		22851,		49391,		Double.MAX_VALUE,		134124,		718022,		2911961,		5225821,		Double.MAX_VALUE,		670,		3980,		11585,		30764,		Double.MAX_VALUE,		1542,		6482,		11906,		29444,		Double.MAX_VALUE};
        final int[] columnIndex = new int[]{6,		6,		6,		6,		6,		10,		10,		10,		10,		10,		12,		12,		12,		12,		12,		14,		14,		14,		14,		14,		20,		20,		20,		20,		20,		22,		22,		22,		22,		22,		26,		26,		26,		26,		26,		48,		48,		48,		48,		48,		50,		50,		50,		50,		50,		58,		58,		58,		58,		58,		60,		60,		60,		60,		60};

        double TOTALE_DISCHI_ROTTI=0,TOTALE_DISCHI_SANI=0;
        //FOR DEBUG
        //System.out.println(valuesAR.length + " " + lowerThreshold.length + " " + upperThreshold.length + " " + columnIndex.length);

        final String data_path = Utils.path;

        System.out.println("Data path: " + data_path);

        //region DISCHI SANI

        //carico il file con i dischi ancora sani all'ultimo giorno
        JavaRDD<String> textFileLastDay = spark_context.textFile(data_path + "lastDay.csv", 1);

        //carichiamo solo i dati che ci servono già convertiti in items
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

        //mettiamo il risultato in un file (nel caso volessimo consultare il risultato, utile per debug)
        List<Tuple2<String, ArrayList<String>>> result = healthy.collect();

        TOTALE_DISCHI_SANI=result.size();

        String filename = data_path + "failedDisks.csv";
        File fileOutput = new File(filename);

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

        //ripetiamo la cosa con i dischi falliti
        JavaRDD<String> textFileFailed = spark_context.textFile(data_path + "failedDisks.csv", 1);

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

        fw = new FileWriter(fileOutput.getAbsoluteFile(), false);
        bw = new BufferedWriter(fw);

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
            bw.write("" + itemset.javaItems().toString().replace("[","").replace("]","").replace(",","") + ", " + ((double)(itemset.freq())/TOTALE_DISCHI_ROTTI)+String.format("%n"));
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
            bw.write("" + itemset.javaItems().toString().replace("[","").replace("]","").replace(",","") + ", " + ((double)(itemset.freq())/TOTALE_DISCHI_SANI)+String.format("%n"));
        }

        bw.close();

        //endregion

        final long endTime = System.currentTimeMillis();
        System.out.println("Total execution time: " + (endTime - startTime));
    }
}
