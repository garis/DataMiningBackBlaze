/*
        final String data_path=Utils.path;
        System.out.println("Data path: "+data_path);

        JavaSparkContext spark_context = new JavaSparkContext(new SparkConf()
                .setAppName("Spark Count")
                .setMaster("local")
        );

        JavaPairRDD<String,String> textFile = spark_context.wholeTextFiles(data_path + "Out1_HardDisksFiles");

        JavaPairRDD<String, ArrayList<String[]>> result = textFile.mapToPair(riga ->
        {
            String[] filename=riga._1().split("/");
            String key=filename[filename.length-1];

            ArrayList<String[]> valori=new ArrayList<>();
            String[] filecontent=riga._2().split("\n");
            for(int i=0;i<filecontent.length;i++)
            {
                valori.add(filecontent[i].split(","));
            }

            Collections.sort(valori,new Comparator<String[]>() {
                public int compare(String[] strings, String[] otherStrings) {
                    return strings[0].compareTo(otherStrings[0]);
                }
            });

            return new Tuple2(key, valori);
        });

        result.foreach((Tuple2<String, ArrayList<String[]>> record) ->
        {
            String filename = data_path + "Out2_HardDisksFilesOrdered/" + record._1() + ".txt";
            File file = new File(filename);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile(),false); // creating fileWriter object with the file
            BufferedWriter bw = new BufferedWriter(fw); // creating bufferWriter which is used to write the content into the file
            String[] row;
            for (Iterator<String[]> iterator = record._2().iterator(); iterator.hasNext(); ) {
                row = iterator.next();
                for(int i=0;i<row.length;i++) {
                    bw.write(row[i] + ",");
                }
                bw.write("\n");
            }
            bw.close();
        });
*/
