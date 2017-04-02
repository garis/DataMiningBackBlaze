/*UNDER TESTING
final String data_path = Utils.path;
        System.out.println("Data path: " + data_path);

        JavaSparkContext spark_context = new JavaSparkContext(new SparkConf()
                .setAppName("Spark Count")
                .setMaster("local")
        );

        JavaPairRDD<String,String> textFile = spark_context.wholeTextFiles(data_path + "Out2_HardDisksFilesOrdered", 1000);

        JavaPairRDD<String, String[]> rows = textFile.mapToPair(file ->
        {
            String[] righe = file._2().split(String.format("%n"));
            //righe.length-3 optimize search ;) (prerequisite=ordered records) I NEED TO THINK ABOUT THIS
            for(int i=0;i<righe.length;i++)
            {
                String[] valori = righe[i].split(",");
                if(valori[3].compareTo("1")==0)
                {
                    //WTF MOMENT.....
                    String[] copia=new String[valori.length];//resolve:java.lang.String cannot be cast to [Ljava.lang.String
                    for(int j=0;j<valori.length;j++)
                    {
                        copia[j]=valori[j];
                    }
                    //END OF WTF MOMENT
                                //the SN
                    return new Tuple2(valori[1], copia);
                }
            }
            String[] empty=new String[1];empty[0]="0";
            return new Tuple2("0", empty);
        });

        //for debug purpouses
        //rows.foreach((Tuple2<String,String[]> disco)->
        //{
        //  System.out.println(disco._1());
        //  for(int i=0;i<disco._2().length;i++)
        //  {
        //      System.out.print(disco._2()[i]);
        //  }
        //  System.out.println("\n");
        //});

        String filename = data_path + "failureStat.csv";
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
                fw = new FileWriter(file.getAbsoluteFile(), false);
                } catch (IOException e) {
                e.printStackTrace();
                }
                BufferedWriter bw = new BufferedWriter(fw); // creating bufferWriter which is used to write the content into the file
                String value = "";

                int anno = 2016;
                for (int mese = 1; mese < 13; mese++) {
        for (int giorno = 1; giorno < 32; giorno++) {
final String data = anno + "-" +String.format("%02d", mese) + "-" + String.format("%02d", giorno);
        java.util.Map<String,Long> number = rows.filter((Tuple2<String, String[]> riga) ->
        {
        if (riga._2()[0].compareTo(data) == 0)
        return true;
        return false;
        }).countByKey();

        String line = data + ","+number.size()+"," +String.format("%n", "");
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