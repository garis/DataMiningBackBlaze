/*
final String data_path=Utils.path;
        System.out.println("Data path: "+data_path);

        JavaSparkContext spark_context = new JavaSparkContext(new SparkConf()
                .setAppName("Spark Count")
                .setMaster("local")
        );

        JavaRDD<String> textFile = spark_context.textFile(data_path + "backblazeHDDstats/Out2_HardDisksFilesOrdered", 60);

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

        String filename = data_path + "backblazeHDDstats/failureStat.csv";
        File file = new File(filename);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        FileWriter fw = null; // creating fileWriter object with the file
        try {
            fw = new FileWriter(file.getAbsoluteFile(),false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter bw = new BufferedWriter(fw); // creating bufferWriter which is used to write the content into the file
        String value = "";

        int anno=2016;
        for (int mese=0;mese<13;mese++){
            for (int giorno=0;giorno<13;giorno++)
            {
                final String data=anno+"-"+System.out.format("%02d",mese)+"-"+System.out.format("%02d",giorno);
                long number=rows.filter((Tuple2<String,ArrayList<String>> riga)->
                {
                    if((riga._2().get(4).compareTo("1")==0) &&(riga._2().get(0).compareTo(data)==0))
                        return true;
                    return false;
                }).count();

                String line=data+","+number;
                System.out.println(line);
                try {
                    bw.write(line);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    bw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
 */