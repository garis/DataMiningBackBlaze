import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class AndamentoItemsets {

    /**
     * Scrive in @param spark_context/andamentoIntemsets.csv basandosi sui valori, impostati a mano, di:
     * valuesAR, lowerThreshold ,upperThreshold e columnIndex, calcolati con TrovaSoglie
     * @param spark_context JavaSparkContext
     * @param path dataset path as String
     * @param numberOfDays quanti giorni pre-fallimento considerare per l'analisi
     */
    public static void AndamentoItemsets(JavaSparkContext spark_context,String path, int numberOfDays) throws IOException {
        final String data_path = path;
        JavaPairRDD<String, String> textFile = spark_context.wholeTextFiles(data_path + "Data", 800);

        //estraiamo tutti i numeri seriali dei dischi rotti
        JavaPairRDD<String, ArrayList<Tuple2<String, String>>> failedDisks = textFile.mapToPair(file ->
        {
            String key = "-1";
            String[] dischi = file._2().split(String.format("%n"));
            ArrayList<Tuple2<String, ArrayList<String>>> lista = new ArrayList<>();
            for (String disco : dischi) {
                key = "0";
                String[] valori = disco.split(",");
                if (valori[4].compareTo("1") == 0) {
                    lista.add(new Tuple2("1", valori[1]));  //prende solo il numero seriale
                }
            }
            return new Tuple2(key, lista);
        });

        //estraimo le tuple dalla lista di tuple
        JavaPairRDD<String, String> failedDisksSNs = failedDisks.flatMapToPair((PairFlatMapFunction<Tuple2<String, ArrayList<Tuple2<String, String>>>, String, String>) t -> {
            List<Tuple2<String, String>> resultFailed = new ArrayList<>();

            for (Tuple2<String, String> lista : t._2()) {
                resultFailed.add(new Tuple2(lista._1(), lista._2()));
            }
            return resultFailed.iterator();
        });

        List<Tuple2<String, String>> result = failedDisksSNs.collect();
        List<String> listOfSN = new ArrayList<>();
        for (Tuple2<String, String> tupla : result) {
            listOfSN.add(tupla._2());
        }

        //ottiene tutti i records dei dischi falliti
        //con record si intende la singola riga di un file .csv
        JavaPairRDD<String, ArrayList<Tuple2<String, String>>> failedDisksBySN = textFile.mapToPair(file ->
        {
            String key = "-1";
            String[] dischi = file._2().split(String.format("%n"));
            ArrayList<Tuple2<String, ArrayList<String>>> lista = new ArrayList<>();
            for (String disco : dischi) {
                String[] valori = disco.split(",");
                if (listOfSN.contains(valori[1]))
                    lista.add(new Tuple2(valori[1], disco));
            }
            return new Tuple2(key, lista);
        });

        //estraggo tutte le Tuple2
        //Result: List<Tuple2<Serial Number,Record>>
        JavaPairRDD<String, String> res1 = failedDisksBySN.flatMapToPair((PairFlatMapFunction<Tuple2<String, ArrayList<Tuple2<String, String>>>, String, String>) t -> {
            List<Tuple2<String, String>> resultFailed = new ArrayList<>();

            for (Tuple2<String, String> lista : t._2()) {
                resultFailed.add(new Tuple2(lista._1(), lista._2()));
            }
            return resultFailed.iterator();
        });

        //region DEBUG1
        //List<Tuple2<String, String>> debug1 = res1.collect();
        //System.out.println("OK");
        //endregion

        //region note utili per l'utilizzo dei metodi
        //public <C> JavaPairRDD<K,C> combineByKey(Function<V,C> createCombiner,
        //        Function2<C,V,C> mergeValue,
        //        Function2<C,C,C> mergeCombiners)

        //Class	                        Function Type
        //Function<T, R>	            T => R
        //DoubleFunction<T>	            T => Double
        //PairFunction<T, K, V>	        T => Tuple2<K, V>
        //FlatMapFunction<T, R>	        T => Iterable<R>
        //DoubleFlatMapFunction<T>	    T => Iterable<Double>
        //PairFlatMapFunction<T, K, V>	T => Iterable<Tuple2<K, V>>
        //Function2<T1, T2, R>	        T1, T2 => R (function of two arguments)

        //endregion

        //raggruppa per numero seriale andando a formare una lista di record per ogni numero seriale
        //un po' come un group by di database raggruppo per numero seriale ottenendo una lista di record
        //result List<Tuple2<SerialNumber, ArrayList<Record of days>>>
        JavaPairRDD<String, ArrayList<String>> res2 = res1.combineByKey((String a) ->
                {//createCombiner
                    ArrayList<String> list = new ArrayList<>();
                    list.add(a);
                    return list;
                },
                (ArrayList<String> listA, String str) ->
                {//mergeValue
                    listA.add(str);
                    return listA;
                }, (ArrayList<String> listA, ArrayList<String> listB) ->
                {//mergeCombiners
                    for (String str : listB)
                        listA.add(str);
                    //inefficient but simple
                    listA.sort((String s, String t1) -> t1.compareTo(s));//inverse order so the recent record are on the top of the list
                    return listA;
                });

        //region DEBUG1
        //List<Tuple2<String, ArrayList<String>>> debug2 = res2.collect();
        //System.out.println("OK");
        //endregion

        //filtra in modo che siano mantenuti solo i numero seriali con almeno numberOfDays record
        JavaPairRDD<String, ArrayList<String>> res3 = res2.filter((Tuple2<String, ArrayList<String>> tupla) -> {
            if (tupla._2().size() >= numberOfDays) return true;
            else return false;
        });

        //region DEBUG4
        //nothing special
        //endregion


        //l'ArrayList<String> di records è già ordinato per data. Ora, per ogni numero seriale, si parte dal primo record
        //(l'ultimo ad essere presente dato che è fallito) al primo record do la chiave numberOfDays, alla secodna key do la chiave
        //numberOfDays-1 e cosi via. quando arrivo a 0 assegno la chiave 0 all  numberOfDays-esimo precendente al fallimento e mi fermo
        //ignorando i record rimanenti
        //l'obiettivo di ciò e poter fare il combinebykey successivo
        //Risultato: JavaPairRDD<ID, Record of one day>
        JavaPairRDD<Integer, String> res4 = res3.flatMapToPair((PairFlatMapFunction<Tuple2<String, ArrayList<String>>, Integer, String>) failedHDDRecords -> {//for each failed HDD...
            List<Tuple2<Integer, String>> dataValori = new ArrayList<>();
            int i = numberOfDays;
            for (String giorno : failedHDDRecords._2()) {//...for each of the numberOfDays days presents on the dataset...
                dataValori.add(new Tuple2(Integer.valueOf(i), giorno));//... and generate Tuple2<counter,Valori>...
                i--;
                if (i < 0) break;//...if we have examined already numberOfDays days stop here and return the list

            }
            return dataValori.iterator();
        });

        //region DEBUG4
        //List<Tuple2<Integer, String>> debug4 = res4.collect();
        //System.out.println("OK");
        //endregion

        //adesso è simile a come fatto per ottenere res2, la si può pensare come un group by di database che raggruppa per chiave (gli id)
        //e ritorna una lista di record.
        //Risultato: JavaPairRDD<ID, ArrayList<record>>
        JavaPairRDD<Integer, ArrayList<String>> res5 = res4.combineByKey((String a) ->
                {//createCombiner
                    ArrayList<String> list = new ArrayList<>();
                    list.add(a);
                    return list;
                },
                (ArrayList<String> listA, String str) ->
                {//mergeValue
                    listA.add(str);
                    return listA;
                }, (ArrayList<String> listA, ArrayList<String> listB) ->
                {//mergeCombiners
                    for (String str : listB)
                        listA.add(str);
                    return listA;
                });

        //region DEBUG5
        //List<Tuple2<Integer, ArrayList<String>>> debug5 = res5.collect();
        //System.out.println("OK");
        //endregion


        //region NEED CHECKING, remember that breakpoints works practically everywhere

        final String[] valuesAR = new String[]{"R_ERR_0",		"R_ERR_1",		"R_ERR_2",		"R_ERR_3",		"R_ERR_4",		"SPIN-UP_0",		"SPIN-UP_1",		"SPIN-UP_2",		"SPIN-UP_3",		"SPIN-UP_4",		"S&S_0",		"S&S_1",		"S&S_2",		"S&S_3",		"S&S_4",		"REALLOC_0",		"REALLOC_1",		"REALLOC_2",		"REALLOC_3",		"REALLOC_4",		"HOURS_0",		"HOURS_1",		"HOURS_2",		"HOURS_3",		"HOURS_4",		"SPIN_ERR_0",		"SPIN_ERR_1",		"SPIN_ERR_2",		"SPIN_ERR_3",		"SPIN_ERR_4",		"POWER-CYCL_0",		"POWER-CYCL_1",		"POWER-CYCL_2",		"POWER-CYCL_3",		"POWER-CYCL_4",		"RETRACT_0",		"RETRACT_1",		"RETRACT_2",		"RETRACT_3",		"RETRACT_4",		"LOAD&UNL_0",		"LOAD&UNL_1",		"LOAD&UNL_2",		"LOAD&UNL_3",		"LOAD&UNL_4",		"UNST-SEC_0",		"UNST-SEC_1",		"UNST-SEC_2",		"UNST-SEC_3",		"UNST-SEC_4",		"ABS-ERR_0",		"ABS-ERR_1",		"ABS-ERR_2",		"ABS-ERR_3",		"ABS-ERR_4"};
        final double[] lowerThreshold = new double[]{1,		111631401,		665349234,		2161913152L,		3523312379L,		1,		2077,		4248,		5664,		7568,		1,		28,		80,		572,		9401,		1,		1676,		8583,		24423,		48796,		1,		7452,		16354,		26375,		39619,		1,		4,		131078,		262150,		327685,		1,		17,		51,		174,		663,		1,		646,		6710,		22851,		49391,		1,		134124,		718022,		2911961,		5225821,		1,		670,		3980,		11585,		30764,		1,		1542,		6482,		11906,		29444};
        final double[] upperThreshold = new double[]{111631401,		665349234,		2161913152L,		3523312379L,		Double.MAX_VALUE,		2077,		4248,		5664,		7568,		Double.MAX_VALUE,		28,		80,		572,		9401,		Double.MAX_VALUE,		1676,		8583,		24423,		48796,		Double.MAX_VALUE,		7452,		16354,		26375,		39619,		Double.MAX_VALUE,		4,		131078,		262150,		327685,		Double.MAX_VALUE,		17,		51,		174,		663,		Double.MAX_VALUE,		646,		6710,		22851,		49391,		Double.MAX_VALUE,		134124,		718022,		2911961,		5225821,		Double.MAX_VALUE,		670,		3980,		11585,		30764,		Double.MAX_VALUE,		1542,		6482,		11906,		29444,		Double.MAX_VALUE};
        final int[] columnIndex = new int[]{6,		6,		6,		6,		6,		10,		10,		10,		10,		10,		12,		12,		12,		12,		12,		14,		14,		14,		14,		14,		20,		20,		20,		20,		20,		22,		22,		22,		22,		22,		26,		26,		26,		26,		26,		48,		48,		48,		48,		48,		50,		50,		50,		50,		50,		58,		58,		58,		58,		58,		60,		60,		60,		60,		60};

        Map<Object,ArrayList<Integer>> dizionario = new HashMap<>();
        for (int i = numberOfDays; i >= 0; i--) {

            final Integer dayUnderTest = Integer.valueOf(i);

            //butta via tutte le tuple che non hanno chiave == dayUnderTest
            JavaPairRDD<Integer, ArrayList<String>> res6 = res5.filter((Tuple2<Integer, ArrayList<String>> tupla) ->
            {
                if (Integer.compare(dayUnderTest, tupla._1()) == 0) return true;
                return false;
            });

            //region DEBUG6
            //List<Tuple2<Integer, ArrayList<String>>> debug6 = res6.collect();
            //System.out.println("OK");
            //endregion

            //converte in tante tuple2<Integer,record> da una sola Tupl2<Integer,lista di record> (una specie di contrario del group by)
            JavaPairRDD<Integer, String> res7 = res6.flatMapToPair((PairFlatMapFunction<Tuple2<Integer, ArrayList<String>>, Integer, String>) t -> {
                List<Tuple2<Integer, String>> resultFailed = new ArrayList<>();

                for (String record : t._2()) {
                    resultFailed.add(new Tuple2(Integer.valueOf(0), record));
                }
                return resultFailed.iterator();
            });

            //region DEBUG7
            //List<Tuple2<Integer, String>> debug7 = res7.collect();
            //System.out.println("OK");
            //endregion

            //converte in lettere per fare la ricerca di itemsets
            JavaPairRDD<Integer, String> res8 = res7.mapToPair((Tuple2<Integer, String> tupla) ->
            {
                StringBuilder totale = new StringBuilder();
                String[] valori = tupla._2().split(",");
                if (valori.length > 6){
                    for (int j = 0; j < columnIndex.length; j++) {
                        if (!valori[columnIndex[j]].isEmpty()) {
                            double value = Double.parseDouble(valori[columnIndex[j]]);
                            //filtra tutti i valori = 0 perchè dopo una breve discussione abbiamo deciso che un valore a zero non
                            //dovrebbe essere presente nell'analisi
                            if ( value > 0 && value >= lowerThreshold[j] && value <= upperThreshold[j]) {
                                totale.append(valuesAR[j]);
                                totale.append(" ");
                            }
                        }
                    }
                }

                return new Tuple2(tupla._1(), totale.toString());
            });

            //region DEBUG8
            //List<Tuple2<Integer, String>> debug8 = res8.collect();
            //System.out.println("OK");
            //endregion

            JavaRDD<List<String>> transactions = res8.map(line -> Arrays.asList(line._2().split(" ")));

            FPGrowth fpg = new FPGrowth()
                    .setMinSupport(0.4)
                    .setNumPartitions(10);
            FPGrowthModel<String> model = fpg.run(transactions);

            for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect()) {

                //qui mette in ordine i vari elementi di un itmeset in maniera che l'ordine con cui le stringhe
                //compaiono nell'itemset non crei itemset doppi
                // in pratica [A, B] è lo stesso di [B, A] e qui fa in modo che:
                //[A, B] diventi [A, B] e [B, A] diventi [A, B]
                //utile per il valore di chiave nella HashMap dopo
                List<String> listItemsets = itemset.javaItems();
                String[] itemsetsarray = listItemsets.toArray(new String[0]);
                Arrays.sort(itemsetsarray);
                String dictKey = Arrays.toString(itemsetsarray);

                Integer value = Integer.valueOf(Long.toString(itemset.freq()));

                //se è la prima volta che vedo la chiave
                if (!dizionario.containsKey(dictKey))
                    if (i == numberOfDays) {//se aggiungo al primo giro del for esterno (non il foreach) inizializzo la lista
                        dizionario.put(dictKey, new ArrayList<>());
                    } else {//altrimenti inizializzo la lista e inserisco i giorni mancanti mettendo zeri
                        dizionario.put(dictKey, new ArrayList<>());
                        for (int h = numberOfDays; h > i; h--) {
                            dizionario.get(dictKey).add(0);
                        }
                    }
                dizionario.get(dictKey).add(value);
            }
            //ora faccio un giro delle lista per mettere valori che eventualmente non erano presenti negli ultimi itemsets
            //così mantengo la coerenza nel file .csv finale
            dizionario.forEach((Object key, ArrayList<Integer> lista) -> {
                if (lista.size() <= numberOfDays - dayUnderTest)
                    lista.add(0);
            });
        }

        //scrive su file
        //N.B: che il csv è separato usando i punti e virgola e non le virgole
        String filename = data_path + "andamentoIntemsets.csv";
        File fileOutput = new File(filename);
        if (!fileOutput.exists()) {
            fileOutput.createNewFile();
        }

        FileWriter fw = new FileWriter(fileOutput.getAbsoluteFile(), false);
        final BufferedWriter bw = new BufferedWriter(fw);

        dizionario.forEach((Object key, ArrayList<Integer> lista) -> {
            try {
                bw.write(key + ";");
                for (Integer value : lista)
                    bw.write(value.toString() + ";");
                bw.write(String.format("%n"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        bw.close();

        //endregion
    }
}
