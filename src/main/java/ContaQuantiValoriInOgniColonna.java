/*
public static void main(String[] args) throws IOException {
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
            long[] contatoreValori = new long[righe[0].split(",").length];

            for (int i = 0; i < contatoreValori.length; i++) {
                contatoreValori[i] = 0;
            }

            for (int i = 1; i < righe.length; i++) {
                String[] valori = righe[i].split(",");
                for (int j = 0; j < valori.length; j++) {
                    if (valori[j].compareTo("") != 0) {
                        contatoreValori[j]++;
                    }
                }
            }
            return new Tuple2("A", contatoreValori);
        });

        JavaPairRDD<String, long[]> rddMapReduce = rows.reduceByKey((long[] vettoreA, long[] vettoreB) ->
        {
            for (int h = 0; h < vettoreB.length; h++) {
                vettoreA[h] = vettoreA[h] + vettoreB[h];
            }
            return vettoreA;
        });
        //List<Tuple2<String, long[]>> fromMapReduceCollect=rddMapReduce.collect();
        Tuple2<String, long[]> colonneValuesCount = rddMapReduce.collect().get(0);

        ArrayList<Tuple2<String, Long>> output = new ArrayList<Tuple2<String, Long>>();
        for (int i = 0; i < colonneValuesCount._2().length; i++) {
            output.add(new Tuple2("" + i, colonneValuesCount._2()[i]));
        }

        Collections.sort(output, new Comparator<Tuple2<String, Long>>() {
            public int compare(Tuple2<String, Long> value, Tuple2<String, Long> otherValue) {
                return -(value._2().compareTo(otherValue._2()));//the minus is for descending order (max to min)
            }
        });

        for (Tuple2<String, Long> elemento : output) {
            System.out.println(elemento._1() + "  " + elemento._2());
        }
    }
        */

        /*
OUTPUT: colonna X ha Y valori
0  24471617
1  24471617
2  24471617
3  24471617
4  24471617
94  24471617
5  24471593
6  24471593
9  24471593
10  24471593
11  24471593
12  24471593
13  24471593
14  24471593
15  24471593
16  24471593
19  24471593
20  24471593
21  24471593
22  24471593
25  24471593
26  24471593
57  24471593
58  24471593
59  24471593
60  24471593
61  24471593
62  24471593
51  24471342
52  24471342
47  24428958
48  24428958
49  24157811
50  24157811
45  14687112
46  14687112
79  14397345
80  14397345
43  14311821
44  14311821
35  14311737
36  14311737
37  14311737
38  14311737
39  14311737
40  14311737
41  14311481
42  14311481
81  14291762
82  14291762
83  14291762
84  14291762
33  13195181
34  13195181
55  10160112
56  10160112
17  9366844
18  9366844
7  9366839
8  9366839
53  2187815
54  2187815
23  1064420
24  1064420
63  1064420
64  1064420
71  351221
72  351221
75  271147
76  271147
67  80074
68  80074
69  80074
70  80074
73  80074
74  80074
77  80074
78  80074
91  45927
92  45927
31  16200
32  16200
85  10047
86  10047
87  10047
88  10047
89  10047
90  10047
27  5
28  5
65  5
66  5
29  0
30  0
93  0
         */