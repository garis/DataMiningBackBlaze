package Varie;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class Statistiche {

    /**
     * Restituisce in output delle statistiche di interesse per ogni giorno
     * le statistiche sono: temperatura media, minima, massima, dev. std. della tempartura, drive count e capacità totale
     * @param spark_context JavaSparkContext
     * @param path dataset path as String
     */
    public static void StatisticheTemperatura(JavaSparkContext spark_context,String path){
        final String data_path = path;

        //indice di colonna contante il valore SMART che indica la temperatura
        final int columnTemperature = 52;

        JavaPairRDD<String, String> textFiles = spark_context.wholeTextFiles(data_path + "Data", 10);

        //di ogni giorno raccoglie tutte le temperature
        JavaPairRDD<String, ArrayList<Double>> temperature = textFiles.mapToPair(fileDisco ->
        {
            String[] tempKey = fileDisco._1().split("/");
            tempKey[0] = tempKey[tempKey.length - 1];//reuse the value at 0 as name

            ArrayList<Double> dischi = new ArrayList<Double>();
            for (String giorno : fileDisco._2().split(String.format("%n"))) {
                if (!giorno.contains("date")) {
                    try {
                        dischi.add(Double.parseDouble(giorno.split(",")[columnTemperature]));
                    } catch (Exception ex) {
                        //se avviene un errore nel leggere la temperatua inserisce un valore di default a 25
                        dischi.add(25D);
                    }
                }
            }
            return new Tuple2(tempKey[0], dischi);
        });

        //di ogni giorno ritorna la media e la dev. std.
        JavaPairRDD<String, ArrayList<Double>> result = temperature.mapToPair((Tuple2<String, ArrayList<Double>> temperatureGiornata) ->
        {
            double mean = 0;
            double sum = 0;
            for (Double temperatura : temperatureGiornata._2())
                sum = sum + temperatura.intValue();

            mean = sum / (double) temperatureGiornata._2().size();

            double DevSTD = 0;
            for (Double temperatura : temperatureGiornata._2())
                DevSTD = Math.pow(temperatura.doubleValue() - mean, 2);

            DevSTD = DevSTD / (double) temperatureGiornata._2().size();
            DevSTD = Math.sqrt(DevSTD);

            ArrayList<Double> valori = new ArrayList<>();
            valori.add(mean);
            valori.add(DevSTD);
            return new Tuple2<String, ArrayList<Double>>(temperatureGiornata._1(), valori);
        });

        JavaPairRDD<String, String> textFile2 = spark_context.wholeTextFiles(data_path + "Data", 10);

        //ora calcola per ogni giorno la temperature minima, massima, il numero di drive e la capacità totale
        JavaPairRDD<String, ArrayList<Double>> stats = textFile2.mapToPair(fileDisco ->
        {
            String[] tempKey = fileDisco._1().split("/");
            tempKey[0] = tempKey[tempKey.length - 1];//reuse the value at 0 as name

            ArrayList<Double> dati = new ArrayList<Double>();
            int count=-1;
            double totalCapacity=-1;
            int minTemp=99999;
            int maxTemp=-99999;
            for (String riga : fileDisco._2().split(String.format("%n"))) {
                if (!riga.contains("date")) {
                    count++;
                    String[] linea=riga.split(",");
                    if(Double.parseDouble(linea[3])>0)
                        totalCapacity=totalCapacity+Double.parseDouble(linea[3]);
                    int temp_TEMP=minTemp;
                    try {
                        temp_TEMP = Integer.parseInt(linea[52]);
                    }
                    catch(java.lang.NumberFormatException ex){}
                    if(temp_TEMP<minTemp)
                        minTemp=temp_TEMP;
                    if(temp_TEMP>maxTemp)
                        maxTemp=temp_TEMP;
                }
            }
            dati.add(Double.parseDouble(""+count));
            dati.add(Double.parseDouble(""+totalCapacity));
            dati.add(Double.parseDouble(""+minTemp));
            dati.add(Double.parseDouble(""+maxTemp));
            return new Tuple2(tempKey[0], dati);
        });

        //e ora fornisce in output i risultati

        List<Tuple2<String,ArrayList<Double>>> risultato=stats.collect();

        JavaPairRDD<String, String> output = result.mapToPair((Tuple2<String, ArrayList<Double>> valori) ->
                new Tuple2<String, String>("0", valori._1() +
                        "," + valori._2().get(0).doubleValue() +
                        "," + valori._2().get(1).doubleValue())
        );

        List<Tuple2<String,String>> temps=  output.reduceByKey((String StrA, String StrB) -> {
                    return StrA + String.format("%n") + StrB;
                }
        ).collect();

        System.out.println("TEMPS");
        for(Tuple2<String,String> tupla: temps)
        {
            System.out.println(tupla._1()+","+tupla._2());
        }

        System.out.println("Data,count,Tot_Capacity,minTemp,MaxTemp");
        for(Tuple2<String,ArrayList<Double>> tupla: risultato)
        {
            System.out.println(tupla._1()+","+tupla._2().get(0)+","+tupla._2().get(1)+","+tupla._2().get(2)+","+tupla._2().get(3));
        }
    }
}
