package Varie;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class StatisticheTemperatura {
    public static void StatisticheTemperatura(JavaSparkContext spark_context,String path){
        final String data_path = path;
        final int columnTemperature = 52;

        System.out.println("Data path: " + data_path);

        JavaPairRDD<String, String> textFile = spark_context.wholeTextFiles(data_path + "Data", 10);

        JavaPairRDD<String, ArrayList<Double>> temperature = textFile.mapToPair(fileDisco ->
        {
            String[] tempKey = fileDisco._1().split("/");
            tempKey[0] = tempKey[tempKey.length - 1];//reuse the value at 0 as name

            ArrayList<Double> dischi = new ArrayList<Double>();
            for (String giorno : fileDisco._2().split(String.format("%n"))) {
                if (!giorno.contains("date")) {
                    try {
                        dischi.add(Double.parseDouble(giorno.split(",")[columnTemperature]));
                    } catch (Exception ex) {//sometimes the temperature is missing :(
                        dischi.add(25D);
                    }
                }
            }
            return new Tuple2(tempKey[0], dischi);
        });

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

        //now tempMin, TempMax, Count drive, total capacity
        JavaPairRDD<String, String> textFile2 = spark_context.wholeTextFiles(data_path + "Data", 10);

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

        List<Tuple2<String,ArrayList<Double>>> risultato=stats.collect();


        JavaPairRDD<String, String> output = result.mapToPair((Tuple2<String, ArrayList<Double>> valori) ->
                new Tuple2<String, String>("0", valori._1() +
                        "," + valori._2().get(0).doubleValue() +
                        "," + valori._2().get(1).doubleValue())
        );

        List<Tuple2<String,String>> temps=  output.reduceByKey((String StrA, String StrB) -> {
                    return StrA + String.format("%n") + StrB;
                }
        ).collect();// .foreach((Tuple2<String, String> allInOne) -> System.out.print(allInOne._2()));

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