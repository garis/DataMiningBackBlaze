import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
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
        final String data_path = Utils.path;
        final int numberOfDays = 5;

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

        JavaPairRDD<String, String> textFile = spark_context.wholeTextFiles(data_path + "TESTDATASET", 400);

        //first get all the Serial Number of all the failed disks
        JavaPairRDD<String, ArrayList<Tuple2<String, String>>> failedDisks = textFile.mapToPair(file ->
        {
            String key = "-1";
            String[] dischi = file._2().split(String.format("%n"));
            ArrayList<Tuple2<String, ArrayList<String>>> lista = new ArrayList<>();
            for (String disco : dischi) {
                key = "0";
                String[] valori = disco.split(",");
                if (valori[4].compareTo("1") == 0) {
                    lista.add(new Tuple2("1", valori[1]));  //take only the Serial Number
                }
            }
            return new Tuple2(key, lista);
        });

        //extract all the Tuple2
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

        //CON RECORD INTENDO LA SINGOLA RIGA DI UN FILE .CSV
        //ottiene tutti i records dei dischi falliti
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

        //estraggo tutte Tuple2
        //Result: List<Tuple2<Serial Number,Record>>
        JavaPairRDD<String, String> res1 = failedDisksBySN.flatMapToPair((PairFlatMapFunction<Tuple2<String, ArrayList<Tuple2<String, String>>>, String, String>) t -> {
            List<Tuple2<String, String>> resultFailed = new ArrayList<>();

            for (Tuple2<String, String> lista : t._2()) {
                resultFailed.add(new Tuple2(lista._1(), lista._2()));
            }
            return resultFailed.iterator();
        });

        //region DEBUG1
        List<Tuple2<String, String>> debug1=res1.collect();
        System.out.println("OK");
        //endregion

        //region cinese di java misto spark
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
        List<Tuple2<String, ArrayList<String>>> debug2=res2.collect();
        System.out.println("OK");
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
        JavaPairRDD<String, String> res4=res3.flatMapToPair((PairFlatMapFunction<Tuple2<String, ArrayList<String>>, String, String>) failedHDDRecords -> {//for each failed HDD...
            List<Tuple2<String, String>> dataValori = new ArrayList<>();
            int i=numberOfDays;
            for (String giorno : failedHDDRecords._2()) {//...for each of the numberOfDays days presents on the dataset...
                dataValori.add(new Tuple2(i, giorno));//... and generate Tuple2<counter,Valori>...
                i--;
                if(i<0) break;//...if we have examined already numberOfDays days stop here and return the list

            }
            return dataValori.iterator();
        });

        //region DEBUG4
        List<Tuple2<String, String>> debug4=res4.collect();
        System.out.println("OK");
        //endregion

        //adesso è simile a come fatto per ottenere res2, la si può pensare come un group by di database che raggruppa per chiave (gli id)
        //e ritorna una lista di record.
        //Risultato: JavaPairRDD<ID, ArrayList<record>>
        JavaPairRDD<String, ArrayList<String>> res5=res4.combineByKey((String a) ->
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
        List<Tuple2<String, ArrayList<String>>> debug5=res5.collect();
        System.out.println("OK");
        //endregion

        //TO BE CONTINUED


    }
}