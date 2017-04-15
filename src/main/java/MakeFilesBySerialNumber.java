/*
crea un file per ogni numero seriale

        final String data_path=Utils.path;
        System.out.println("Data path: "+data_path);

        JavaSparkContext spark_context = new JavaSparkContext(new SparkConf()
                .setAppName("Spark Count")
                .setMaster("local")
        );

        JavaRDD<String> textFile = spark_context.textFile(data_path + "Data", 60);

        JavaPairRDD<String, ArrayList<String>> rows = textFile.mapToPair(riga ->
        {
            String key = "";
            ArrayList<String> valoriSMART = new ArrayList<String>();
            if (riga.contains("serial_number")) {
                key = "0";
                valoriSMART.add("0");
            } else {
                String[] valori = riga.split(",");
                key = ""+valori[1];
                for (int i = 0; i < valori.length; i++) {
                    if (i != 1) {
                        valoriSMART.add(""+valori[i]);
                    }
                }
                valoriSMART.add("$n");
            }
            return new Tuple2(key, valoriSMART);
        });

        JavaPairRDD<String, ArrayList<String>> result = rows.reduceByKey((ArrayList<String> tuplaA, ArrayList<String> tuplaB) ->
        {
            for(Iterator<String> i = tuplaB.iterator(); i.hasNext(); ) {
                tuplaA.add(i.next());
            }
            return tuplaA;
        });

        result.foreach((Tuple2<String, ArrayList<String>> record) ->
        {
            String filename = data_path + "Out1_HardDisksFiles/" + record._1() + ".txt";
            File file = new File(filename);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile(),false); // creating fileWriter object with the file
            BufferedWriter bw = new BufferedWriter(fw); // creating bufferWriter which is used to write the content into the file
            String value = "";
            for (Iterator<String> i = record._2().iterator(); i.hasNext(); ) {
                value = (String) i.next();
                if (value.contains("$n"))
                    bw.write("\n");
                else
                    bw.write(value+",");
            }
            bw.close();
        });
*/