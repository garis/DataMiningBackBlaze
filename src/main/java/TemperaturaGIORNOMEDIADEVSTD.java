/*
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException {
        final String data_path = Utils.path;
        final int columnTemperature = 29;

        System.out.println("Data path: " + data_path);

        JavaSparkContext spark_context = new JavaSparkContext(new SparkConf()
                .setAppName("Spark Count")
                .setMaster("local")
                //uffa
                .set("spark.executor.memory", "2g")
        );

        //fix filesystem errors when using java .jar execution
        spark_context.hadoopConfiguration().set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        spark_context.hadoopConfiguration().set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );

        JavaPairRDD<String, String> textFile = spark_context.wholeTextFiles(data_path + "filteredColumns/", 10000);

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

        temperature.cache();
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

        JavaPairRDD<String, String> output = result.mapToPair((Tuple2<String, ArrayList<Double>> valori) ->
                new Tuple2<String, String>("0", valori._1() +
                        "," + valori._2().get(0).doubleValue() +
                        "," + valori._2().get(1).doubleValue())
        );

        output.reduceByKey((String StrA, String StrB) -> {
            return StrA + String.format("%n") + StrB;
                }
        ).foreach((Tuple2<String, String> allInOne) -> System.out.print(allInOne._2()));
    }
}
*/
/*
OUTPUT (fogli di calcolo per ulteriori dettagli):
2016-11-14.csv,28.27084350798856,0.010194710241271412
2016-03-05.csv,27.459798148356146,0.009853154552346179
2016-02-18.csv,28.15250566274821,0.01688470865093049
2016-04-11.csv,29.392505487613672,0.021352480714019555
2016-01-06.csv,27.28260453794662,0.02384007792144366
2016-04-01.csv,28.103634515186172,0.011543794775863212
2016-06-15.csv,27.848869139010276,0.027476724044595634
2016-02-02.csv,27.187954202660084,0.0033618816661298264
2016-07-30.csv,26.976954064710824,0.02649715545330306
2016-10-27.csv,25.03313998123347,0.014957253966680585
2016-04-25.csv,27.94157077766623,0.015490527671139794
2016-06-27.csv,28.095241118765674,0.020300248750747096
2016-06-05.csv,27.542741717478147,0.028899326521260045
2016-10-03.csv,24.97493578828894,0.01528973396563177
2016-05-12.csv,27.286331736995873,0.026053693386797062
2016-04-09.csv,29.652476828657313,0.01840894758591827
2016-12-01.csv,27.538349039031996,0.005429221747903351
2016-01-23.csv,27.485536659302266,0.006267484181607065
2016-12-23.csv,27.996958670504537,0.007380701444355904
2016-01-15.csv,27.274578273483783,0.007191692293573951
2016-07-27.csv,27.36809929603557,0.012714426346848226
2016-07-04.csv,28.0385411938266,0.02245278974872204
2016-05-17.csv,27.543643263757115,0.028935840225999494
2016-12-24.csv,27.96277137387479,0.007506622038764689
2016-08-02.csv,27.843661130701854,0.02698526974990183
2016-05-23.csv,28.376088069636456,0.029586070693453592
2016-09-01.csv,24.202759993078388,0.01062215809013504
2016-01-26.csv,27.198838822380928,0.015730798436818218
2016-09-27.csv,24.19788750685406,7.51701631521174E-4
2016-11-13.csv,27.879369287657852,0.007921574194939406
2016-05-22.csv,28.42275001507023,0.025533141345206373
2016-12-07.csv,27.848939605693428,0.011607158890657008
2016-05-14.csv,28.230336571720756,0.030130821775403504
2016-06-20.csv,27.52222839271622,0.02858654175157058
2016-11-17.csv,28.081011207691663,0.014650246247607841
2016-07-03.csv,28.082583392476934,0.022294187407266415
2016-11-12.csv,27.66513172583166,0.00498640889671812
2016-02-04.csv,27.70087242685498,0.009518503885205304
2016-09-04.csv,24.48556555344062,0.005750869452994602
2016-03-16.csv,28.19508046788505,0.004787105218544405
2016-03-27.csv,28.106025088697415,0.012361364925017604
2016-07-12.csv,28.722126036344186,0.02355330989330959
2016-06-21.csv,27.79820536053547,0.027531517938680614
2016-06-19.csv,27.515491187560283,0.02861229720474482
2016-01-10.csv,27.640417985186215,0.02651806225997624
2016-07-25.csv,27.19347053026949,0.008289296029181997
2016-12-15.csv,27.52024047008373,0.009134993370102693
2016-12-13.csv,28.14863380568174,0.01049993731540232
2016-07-21.csv,27.575665517436274,0.013440831972699279
2016-09-18.csv,24.06614527719258,0.014943262767378495
2016-08-09.csv,26.71930173995931,0.02746182215351706
2016-03-10.csv,28.010581755704354,0.008056713042682186
2016-05-30.csv,27.369859908419095,0.02966187926630774
2016-10-21.csv,24.930415040794607,0.015329661927201171
2016-08-06.csv,26.522006838201204,0.028463283728853123
2016-01-12.csv,26.86627836216639,0.021406307239940223
2016-05-10.csv,28.147170214715494,0.026779901601908924
2016-05-27.csv,27.395880212030143,0.025664345404347987
2016-01-29.csv,27.51382133315065,0.0021264262847027343
2016-09-11.csv,24.50085078156544,0.009490196773734775
2016-03-18.csv,28.289614500811492,0.009178230854574974
2016-11-21.csv,27.351365326029235,0.006163044914397418
2016-07-11.csv,28.18995697863037,0.02182084401476471
2016-04-04.csv,28.06044287711272,0.011715939972513122
2016-05-18.csv,27.597783199301226,0.02484504550818676
2016-12-26.csv,27.962048331727704,0.00750694238290586
2016-09-28.csv,24.28168018239012,0.010325894112156665
2016-09-19.csv,24.048353582869183,0.01501084685018917
2016-05-01.csv,27.0537312970478,0.015953885477523775
2016-01-22.csv,27.56625311037255,0.0023620755295939272
2016-04-14.csv,27.724884395328786,0.010788343376818822
2016-07-07.csv,28.174718607354485,0.025697843319404255
2016-10-05.csv,25.0879195405616,0.018659181442142608
2016-02-24.csv,28.433954599372775,0.013842428040915115
2016-03-06.csv,28.780043676536707,0.011140155784428602
2016-09-22.csv,24.35379092599801,0.010047551019053306
2016-11-03.csv,24.41324746225369,0.013524038753298174
2016-04-22.csv,28.157263109120397,0.012408167787832085
2016-11-09.csv,26.1353415478962,0.0070255008373245635
2016-07-15.csv,26.903005713804383,0.02659132235935541
2016-01-08.csv,26.860697209423627,0.025599323657438965
2016-02-11.csv,28.32256484733856,0.011002703940398196
2016-02-14.csv,27.94091223803975,0.004352199039755847
2016-07-24.csv,27.214802114587798,0.008371763145038843
2016-11-07.csv,24.71100031250888,0.012396060057790289
2016-01-17.csv,27.56036791042181,0.006002950082877729
2016-11-22.csv,27.99086056066408,0.0037724377557446674
2016-08-16.csv,26.280485233550273,0.025192886487848728
2016-04-30.csv,28.170009905894005,0.01640566873613773
2016-11-15.csv,28.1368079995531,0.0069628809371788634
2016-08-11.csv,26.413015042511446,0.021066619126600447
2016-05-15.csv,28.49753939229762,0.029104894867783605
2016-10-22.csv,24.86158786087747,0.015599000320448071
2016-08-10.csv,26.88111013165752,0.026842911266401617
2016-07-02.csv,28.030274267580893,0.022469268826506548
2016-08-15.csv,26.432182066095955,0.024624489017093926
2016-08-29.csv,25.119740027668897,0.007137780662002628
2016-12-29.csv,28.30806620232713,0.006234306435158748
2016-07-10.csv,28.225371806279394,0.02170192194340488
2016-01-18.csv,27.860626086956522,0.013092094239686537
2016-05-04.csv,26.955010537246878,0.01545562874615995
2016-12-28.csv,28.115556943463858,0.00694369675970995
2016-05-21.csv,28.204985231177286,0.026378512742049547
2016-01-11.csv,27.208119620968443,0.024150660989646237
2016-11-16.csv,28.633889168202895,0.008842324356277208
2016-04-23.csv,27.99223106031354,0.0156896245678954
2016-03-21.csv,27.628932029481188,0.006527383375647044
2016-01-25.csv,27.52645190018668,0.014374998182261275
2016-02-07.csv,27.62838729581955,0.0015384953013697538
2016-01-19.csv,27.149195861949057,0.0035476358215278244
2016-09-03.csv,24.54625944511738,0.009317763231487084
2016-11-24.csv,27.602351922459185,0.0052232196274863765
2016-02-21.csv,27.758594856928813,0.007096937876211688
2016-05-02.csv,26.90830519500635,0.019317150849858838
2016-06-24.csv,28.155621470718863,0.016550613667296297
2016-12-21.csv,28.147176625527813,0.0068271398695221655
2016-06-02.csv,27.43313545923694,0.02922331794169348
2016-04-29.csv,27.626870295220098,0.014273929151128928
2016-07-17.csv,27.504357129784747,0.028124111296585427
2016-12-30.csv,28.65045076847879,0.004972748098426368
2016-11-27.csv,26.38039971508778,0.006052688178644167
2016-01-27.csv,27.349039302058515,0.010975110732270343
2016-02-29.csv,27.810063659541715,0.01125259424727737
2016-02-06.csv,27.580645161290324,0.006544006615934577
2016-03-02.csv,27.54238564447649,0.006173742150909199
2016-08-13.csv,26.437123980882767,0.024605787528609306
2016-09-26.csv,24.284205362038612,0.006517670818209123
2016-06-30.csv,28.301393406530973,0.021476974101599624
2016-06-04.csv,27.353585194294883,0.029612142348579798
2016-02-03.csv,27.301955675916563,0.0028899386395269855
2016-12-19.csv,27.95250953201536,0.011225651575441982
2016-04-06.csv,27.650860013353256,0.013353258769383737
2016-08-31.csv,24.183148756993713,0.010696629229371899
2016-03-01.csv,27.839539476853414,0.011378475246514596
2016-08-08.csv,26.627566191011397,0.027807836401105006
2016-12-08.csv,27.452017803348983,0.009385995121545178
2016-07-26.csv,27.67660034725187,0.01009743146858006
2016-08-04.csv,26.611127059769192,0.02799377881793113
2016-12-22.csv,28.124462004263233,0.006910836944263942
2016-10-01.csv,25.067891258549537,0.018735261496433946
2016-06-07.csv,27.75583455981738,0.028073521728560508
2016-05-20.csv,28.04350776655105,0.027014739910520155
2016-12-14.csv,26.838803782954994,0.007960922593058887
2016-05-26.csv,28.29266268260292,0.029940947686508797
2016-10-28.csv,25.06259863229879,0.014846284111893638
2016-09-29.csv,24.265966350177486,0.010385585163398204
2016-09-10.csv,24.447640883659226,0.009692254497105379
2016-12-20.csv,28.04066540929999,0.010900923024812869
2016-07-05.csv,28.024809918293236,0.02250450619138421
2016-03-31.csv,28.054565383149065,0.01173936539463908
2016-09-15.csv,24.84642357253142,0.015778018296677583
2016-04-21.csv,27.044465334900117,0.016009055332755224
2016-10-17.csv,24.925506555423123,0.015348042741612146
2016-02-13.csv,27.92299508587061,0.003792941879519337
2016-12-18.csv,28.018046867579415,0.007300766788132102
2016-02-27.csv,27.59859832568881,0.010406555266045298
2016-10-29.csv,24.49857922255058,0.013197975936982898
2016-06-01.csv,27.47544969125675,0.029059899890648423
2016-07-23.csv,27.04309212405049,0.011498850642612987
2016-08-07.csv,26.672252411163903,0.027637516606723842
2016-04-15.csv,27.80500039188024,0.011105538074052517
2016-05-11.csv,27.217900063251108,0.026319255785153286
2016-11-08.csv,26.0857548442047,0.007212278192423161
2016-04-27.csv,28.08520997233342,0.016060741765269497
2016-01-21.csv,27.405540378290905,0.0016916763657032741
2016-09-06.csv,24.449097306339045,0.005889352917060789
2016-02-09.csv,28.03386884471295,0.012184756530747746
2016-04-24.csv,28.080421956568745,0.01603609440745128
2016-04-19.csv,26.578114483384727,0.014156242199247064
2016-11-02.csv,24.460917285109044,0.013344297183487015
2016-02-23.csv,28.37527482370568,0.013621167857168494
2016-01-24.csv,27.65269143160527,0.013852566080408624
2016-11-26.csv,26.28818033267692,0.0026601764187552876
2016-01-13.csv,27.401147526732156,0.02751552839032002
2016-10-30.csv,24.38548533757672,0.013624262997971938
2016-01-01.csv,26.867547137023198,0.029732752310450077
2016-02-08.csv,27.903467425398077,0.0037404006194594877
2016-09-30.csv,25.00753225015151,0.015165912032891048
2016-03-08.csv,27.75118335125074,0.0030089971658728396
2016-03-11.csv,28.513405965694847,0.010067890102824236
2016-05-24.csv,28.28664411489323,0.026088652057317457
2016-04-13.csv,28.041643148322358,0.016003527038725755
2016-10-20.csv,24.812233951983654,0.015774724679548897
2016-06-09.csv,27.719165397138333,0.028137979014913255
2016-07-29.csv,27.05523950392536,0.011522070681433365
2016-10-13.csv,24.51939217496272,0.013116637021360875
2016-07-18.csv,27.471717300114403,0.02829241953593858
2016-06-12.csv,27.36575823786465,0.029332976597059357
2016-12-12.csv,26.200287481015405,0.010309796519641068
2016-01-03.csv,27.21535951116203,0.028267863123959243
2016-04-18.csv,27.895114424899358,0.011458186654257532
2016-03-17.csv,28.041120984718226,0.008182110828360652
2016-12-16.csv,27.522329965416695,0.012807121538422837
2016-11-11.csv,27.5734544680247,0.0053747782511637575
2016-01-20.csv,27.533472439575725,0.0019453852614854182
2016-10-12.csv,24.61918084471838,0.012740674850550271
2016-11-23.csv,27.10670242035726,0.0070755358502424094
2016-08-12.csv,26.287208321619342,0.025167857092060146
2016-07-19.csv,27.609694990639998,0.013542462279534496
2016-06-23.csv,27.780483894476024,0.027562231740887864
2016-02-19.csv,28.06477960384908,0.012461954946558952
2016-07-01.csv,27.946870120652946,0.022805494505680052
2016-12-10.csv,26.22550681402129,0.006534435056609692
2016-05-09.csv,28.54860877798479,0.025211135769340226
2016-09-16.csv,24.124455282675825,0.014721764568969786
2016-05-03.csv,26.846018753245165,0.018937564398810326
2016-02-15.csv,27.499400509988686,0.0020571558826220134
2016-06-26.csv,28.17736818729776,0.02061992924165262
2016-05-19.csv,27.469006957621758,0.02534478672041038
2016-04-03.csv,28.036758164951074,0.011810338092485544
2016-01-31.csv,28.184889361191573,0.009045551937249511
2016-03-13.csv,28.38300466922324,0.005539868692713556
2016-04-02.csv,28.087828821959587,0.011606790167796522
2016-02-26.csv,28.244596697848305,0.00899112725422829
2016-03-28.csv,27.960833729592647,0.012099688699383809
2016-03-04.csv,27.393961743815037,0.009585898266282448
2016-03-25.csv,27.975099064828022,0.016024147423468284
2016-08-22.csv,25.226113063646324,0.0029192059055958646
2016-08-23.csv,24.170385135616147,0.0069037155103302
2016-05-08.csv,28.41241867994258,0.02965120797016615
2016-01-28.csv,27.161527518296968,0.007611368618399645
2016-10-15.csv,24.911558544752825,0.015400582932642057
2016-03-30.csv,27.643058816064972,0.013369705838374615
2016-12-17.csv,28.04164235606996,0.010897324370719394
2016-09-02.csv,24.299370869527575,0.0102586937183806
2016-02-28.csv,28.43075887059276,0.009733241269859708
2016-10-07.csv,25.141374322906326,0.014530158099548343
2016-09-21.csv,24.330839208223406,0.013931658165139228
2016-09-17.csv,24.49689763643185,0.017105624541004744
2016-10-09.csv,24.70956070151541,0.01239462124035058
2016-01-14.csv,27.25833260888464,0.015601796788826392
2016-04-10.csv,29.666442259519037,0.018464206084609695
2016-10-10.csv,25.152905285562255,0.014491566576304689
2016-05-16.csv,27.56070912593306,0.024980267604187888
2016-09-12.csv,24.575806079483186,0.01300293470170869
2016-07-28.csv,26.682486631016044,0.010116326567804812
2016-12-25.csv,27.909211793918505,0.00401799725797665
2016-06-29.csv,28.096997329697178,0.02601593113088175
2016-02-20.csv,28.219520536790334,0.012992715865500258
2016-09-05.csv,24.443906096787217,0.005909065865682322
2016-09-09.csv,24.425771843050168,0.009775369838573411
2016-07-14.csv,28.414356108997488,0.02842221564152504
2016-08-01.csv,27.269631044708028,0.029188041721784994
2016-12-06.csv,27.01993242784841,0.007293722127867572
2016-06-03.csv,27.319842008830687,0.029763136143731418
2016-05-29.csv,27.369038263918274,0.02577741665165868
2016-10-18.csv,24.947683575736075,0.019031499160134108
2016-04-05.csv,28.082917368772453,0.011630615436130714
2016-03-19.csv,28.008976026462417,0.008050278636476206
2016-08-14.csv,26.33189485521507,0.025000316619279067
2016-09-25.csv,24.280225254494262,0.006535006145746824
2016-02-01.csv,27.890937784105528,0.01611503241337739
2016-05-31.csv,27.366066705958804,0.025789163248222734
2016-10-08.csv,25.095516292788066,0.01470284249122379
2016-03-24.csv,27.61711233337032,0.01346823714992305
2016-12-31.csv,28.279975017989763,0.006337814652517224
2016-02-10.csv,26.89938700035463,0.00863222691104835
2016-06-28.csv,28.417403708987163,0.013605547495537983
2016-04-08.csv,29.504180841255206,0.00995100808003942
2016-08-03.csv,28.023854541318965,0.02630298784805822
2016-07-06.csv,28.206359119943222,0.02559540978830123
2016-08-26.csv,24.059368105325394,0.014964149181619434
2016-08-30.csv,24.13613765077169,0.010871621109021897
2016-07-13.csv,28.51537812763565,0.02431239406778128
2016-10-25.csv,24.962950325570816,0.01522190774841565
2016-01-16.csv,27.18855950621577,0.011722988358626904
2016-10-19.csv,24.84462795845394,0.015652701021236285
2016-12-27.csv,27.927375666978943,0.007637045394324635
2016-05-13.csv,27.386560443366818,0.029545653090081336
2016-03-12.csv,28.518909551850843,0.006084259627328715
2016-09-20.csv,24.21059998268448,0.0067972704990017065
2016-12-11.csv,26.06603927099154,0.007121710339133447
2016-08-28.csv,25.13977115517639,0.007061739158878016
2016-07-31.csv,27.078267563868614,0.029910584888232845
2016-02-22.csv,28.311238864550592,0.013362746071060834
2016-08-20.csv,25.187125854934806,0.010606919072368555
2016-09-23.csv,24.96609044472021,0.007726072026355239
2016-10-04.csv,25.075163775937202,0.018707635885941994
2016-05-06.csv,28.158484329089127,0.026763904284105418
2016-04-28.csv,28.00040271367058,0.01574400523398165
2016-03-29.csv,27.4949278808052,0.013954577584023442
2016-03-20.csv,27.645631613597317,0.00659430120018915
2016-04-16.csv,27.76078068952554,0.010926525107260004
2016-11-01.csv,24.369158147668948,0.013690766443672033
2016-06-25.csv,28.25222697733397,0.012947937198646455
2016-06-11.csv,27.469334198266203,0.02507569357558241
2016-06-14.csv,27.67767253843203,0.02428397608843583
2016-05-25.csv,28.561094582962077,0.028908415380488543
2016-06-13.csv,27.460866614538,0.025116910049979044
2016-11-20.csv,27.359428715168114,0.013609445862523634
2016-11-25.csv,26.265265363128492,0.0027458322572905774
2016-06-22.csv,28.007136810765054,0.026714847094912404
2016-09-08.csv,24.449313606737036,0.009685902546994
2016-03-15.csv,28.061903310174415,0.008259316725122264
2016-03-14.csv,28.37162845177543,0.005494299250843177
2016-07-20.csv,27.481515998757377,0.013082675413201137
2016-05-05.csv,27.989646009590423,0.019498839654842164
2016-10-24.csv,24.888349859868264,0.01550837495125015
2016-01-04.csv,27.13568031107871,0.028599842500830797
2016-08-27.csv,25.156055777466943,0.010799589189668142
2016-09-07.csv,24.411850377804694,0.009828164326212651
2016-11-19.csv,27.952877386176247,0.011390973285452603
2016-01-05.csv,27.460867601327287,0.02725561371778188
2016-02-17.csv,27.624769914044954,0.0025674199227286028
2016-02-16.csv,27.9787729874867,8.72299603446256E-5
2016-12-03.csv,26.70385384350224,0.0010996696317431961
2016-11-10.csv,28.35216126055788,0.009976229815557522
2016-12-02.csv,26.798683738513756,0.00817672405276389
2016-11-30.csv,26.716345952291118,0.004798614512786278
2016-03-09.csv,28.07262946607788,0.0042982617111375476
2016-05-28.csv,27.39871542995315,0.025662436486760865
2016-10-23.csv,25.006298428947183,0.015058782589783567
2016-08-21.csv,25.24451843314694,0.0028497378206926733
2016-06-08.csv,27.798378013066007,0.03178421916720263
2016-07-16.csv,27.526118825532155,0.0280333845315484
2016-04-12.csv,29.348149890247726,0.017217189094464264
2016-09-13.csv,25.077421122454865,0.011098116282963555
2016-06-10.csv,27.50234997716323,0.028779062456190694
2016-10-02.csv,25.178450838359645,0.018315286441693147
2016-08-24.csv,24.17211731044349,0.010696034660189703
2016-12-04.csv,26.776642537056187,0.011969185608116878
2016-03-26.csv,27.967070563079115,0.011808446939473737
2016-03-22.csv,28.025983834423357,0.008121431504466842
2016-08-05.csv,26.365716840192658,0.02899062354673391
2016-11-06.csv,24.838404500127844,0.011915880638088556
2016-11-04.csv,24.353166140635217,0.009980026029362755
2016-01-07.csv,27.04304963922455,0.02066921619082359
2016-04-07.csv,26.8014020920103,0.016740107776179743
2016-10-11.csv,25.54966981467017,0.016771026384034472
2016-10-06.csv,24.916669071599664,0.01930969879396508
2016-02-12.csv,27.539431910294514,0.010111506226139383
2016-09-24.csv,24.33405530286622,0.006330456277890068
2016-03-03.csv,27.54253853302144,0.010177158427390818
2016-07-22.csv,26.99615581025874,0.011284495733240271
2016-11-05.csv,24.87094987144704,0.011793135152249943
2016-12-05.csv,26.561210833482804,0.009058747631949929
2016-11-28.csv,26.294367397103393,0.002637054434853774
2016-08-25.csv,24.008574351879872,0.007559729649361174
2016-10-16.csv,25.025441284976445,0.014971602695402889
2016-08-18.csv,26.77745059176624,0.030938086034696905
2016-10-31.csv,24.41417083428052,0.01351613817684921
2016-02-05.csv,28.101760279725074,0.007858807994621974
2016-01-30.csv,27.66367859040502,0.006887713109734516
2016-07-08.csv,28.029736594275278,0.02255226929854719
2016-05-07.csv,28.29268180705191,0.026238925810309705
2016-02-25.csv,27.884175040307476,0.007603725870279908
2016-10-26.csv,25.058133583553698,0.018633566778386346
2016-06-06.csv,27.52640194635509,0.0328379771337872
2016-04-26.csv,27.798365922219133,0.01492761169581888
2016-12-09.csv,26.09635922000733,0.0070124839675002925
2016-11-18.csv,27.64621705469689,0.01627562507515013
2016-10-14.csv,24.78980078324536,0.01585922726552885
2016-01-09.csv,27.272607145961924,0.02388176449105534
2016-03-23.csv,27.456617530511966,0.018088353394259838
2016-09-14.csv,24.747799486306313,0.012353909910950006
2016-01-02.csv,26.887531497089235,0.025480786973840878
2016-03-07.csv,28.264837176440363,0.009075626827324745
2016-04-20.csv,26.998480243161094,0.015826910283822472
2016-06-17.csv,28.085753641452477,0.02646786899716913
2016-06-16.csv,28.195597651293856,0.026135619348494923
2016-07-09.csv,28.286666292544684,0.021399904668418655
2016-11-29.csv,26.587710355422107,0.009014834912904186
2016-08-17.csv,26.51458803611738,0.028116020580382317
2016-04-17.csv,27.494932723484908,0.009874360954316666
2016-06-18.csv,27.8727950310559,0.027246570490574877
2016-08-19.csv,26.292391273511306,0.017730131677938476
 */