/*
final String data_path = Utils.path;
        System.out.println("Data path: " + data_path);

        JavaSparkContext spark_context = new JavaSparkContext(new SparkConf()
        .setAppName("Spark Count")
        .setMaster("local")
        );

        JavaPairRDD<String, String> textFile = spark_context.wholeTextFiles(data_path + "Data", 1000);

        JavaPairRDD<String, long[]> rows = textFile.mapToPair(file ->
        {
        String[] righe = file._2().split(String.format("%n"));
        int[] contatoreValori=new int[righe[0].split(",").length];

        for (int i = 0; i < contatoreValori.length; i++) {
        contatoreValori[i]=0;
        }

        for (int i = 1; i < righe.length; i++) {
        String[] valori = righe[i].split(",");
        for (int j = 0; j < valori.length; j++) {
        if(valori[j].compareTo("")!=0){
        contatoreValori[j]++;
        }
        }
        }
        return new Tuple2("A",contatoreValori );
        });

        JavaPairRDD<String, long[]> result = rows.reduceByKey((long[] vettoreA, long[] vettoreB) ->
        {
        for(int h=0;h<vettoreB.length;h++ ) {
        vettoreA[h]=vettoreA[h]+vettoreB[h];
        }
        return vettoreA;
        });

        result.foreach((Tuple2<String, long[]> bla)->
        {
        System.out.println("CHIAVE "+bla._1());
        for(int i=0;i<bla._2().length;i++) {
        System.out.println("COLONNA "+i+" ha "+bla._2()[i]+" valori");
        }
        });
        */

        /*
        OUTPUT:
        CHIAVE A
COLONNA 0 ha 24471617 valori
COLONNA 1 ha 24471617 valori
COLONNA 2 ha 24471617 valori
COLONNA 3 ha 24471617 valori
COLONNA 4 ha 24471617 valori
COLONNA 5 ha 24471593 valori
COLONNA 6 ha 24471593 valori
COLONNA 7 ha 9366839 valori
COLONNA 8 ha 9366839 valori
COLONNA 9 ha 24471593 valori
COLONNA 10 ha 24471593 valori
COLONNA 11 ha 24471593 valori
COLONNA 12 ha 24471593 valori
COLONNA 13 ha 24471593 valori
COLONNA 14 ha 24471593 valori
COLONNA 15 ha 24471593 valori
COLONNA 16 ha 24471593 valori
COLONNA 17 ha 9366844 valori
COLONNA 18 ha 9366844 valori
COLONNA 19 ha 24471593 valori
COLONNA 20 ha 24471593 valori
COLONNA 21 ha 24471593 valori
COLONNA 22 ha 24471593 valori
COLONNA 23 ha 1064420 valori
COLONNA 24 ha 1064420 valori
COLONNA 25 ha 24471593 valori
COLONNA 26 ha 24471593 valori
COLONNA 27 ha 5 valori
COLONNA 28 ha 5 valori
COLONNA 29 ha 0 valori
COLONNA 30 ha 0 valori
COLONNA 31 ha 16200 valori
COLONNA 32 ha 16200 valori
COLONNA 33 ha 13195181 valori
COLONNA 34 ha 13195181 valori
COLONNA 35 ha 14311737 valori
COLONNA 36 ha 14311737 valori
COLONNA 37 ha 14311737 valori
COLONNA 38 ha 14311737 valori
COLONNA 39 ha 14311737 valori
COLONNA 40 ha 14311737 valori
COLONNA 41 ha 14311481 valori
COLONNA 42 ha 14311481 valori
COLONNA 43 ha 14311821 valori
COLONNA 44 ha 14311821 valori
COLONNA 45 ha 14687112 valori
COLONNA 46 ha 14687112 valori
COLONNA 47 ha 24428958 valori
COLONNA 48 ha 24428958 valori
COLONNA 49 ha 24157811 valori
COLONNA 50 ha 24157811 valori
COLONNA 51 ha 24471342 valori
COLONNA 52 ha 24471342 valori
COLONNA 53 ha 2187815 valori
COLONNA 54 ha 2187815 valori
COLONNA 55 ha 10160112 valori
COLONNA 56 ha 10160112 valori
COLONNA 57 ha 24471593 valori
COLONNA 58 ha 24471593 valori
COLONNA 59 ha 24471593 valori
COLONNA 60 ha 24471593 valori
COLONNA 61 ha 24471593 valori
COLONNA 62 ha 24471593 valori
COLONNA 63 ha 1064420 valori
COLONNA 64 ha 1064420 valori
COLONNA 65 ha 5 valori
COLONNA 66 ha 5 valori
COLONNA 67 ha 80074 valori
COLONNA 68 ha 80074 valori
COLONNA 69 ha 80074 valori
COLONNA 70 ha 80074 valori
COLONNA 71 ha 351221 valori
COLONNA 72 ha 351221 valori
COLONNA 73 ha 80074 valori
COLONNA 74 ha 80074 valori
COLONNA 75 ha 271147 valori
COLONNA 76 ha 271147 valori
COLONNA 77 ha 80074 valori
COLONNA 78 ha 80074 valori
COLONNA 79 ha 14397345 valori
COLONNA 80 ha 14397345 valori
COLONNA 81 ha 14291762 valori
COLONNA 82 ha 14291762 valori
COLONNA 83 ha 14291762 valori
COLONNA 84 ha 14291762 valori
COLONNA 85 ha 10047 valori
COLONNA 86 ha 10047 valori
COLONNA 87 ha 10047 valori
COLONNA 88 ha 10047 valori
COLONNA 89 ha 10047 valori
COLONNA 90 ha 10047 valori
COLONNA 91 ha 45927 valori
COLONNA 92 ha 45927 valori
COLONNA 93 ha 0 valori
COLONNA 94 ha 24471617 valori
         */