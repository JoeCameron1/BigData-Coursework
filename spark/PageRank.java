package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import com.google.common.collect.Iterables;
import utils.ISO8601;

import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

public class PageRank {

    public static void main(String[] args) throws ParseException
    {
        if (args.length != 4)
        {
            usage();
            System.exit(1);
        }
        Integer iterations = Integer.parseInt(args[2]);
        long time = ISO8601.toTimeMS(args[3]);

        SparkConf conf = new SparkConf().setAppName("PageRank-v0");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.hadoopConfiguration().set("textinputformat.record.delimiter","\n\n");
        JavaRDD<String> inputData = sc.textFile(args[0]);
            JavaPairRDD<String, Tuple2<Long, ? extends List<? extends Object>>> records = inputData.mapToPair( (String f) -> {
                String revision = "";
                Long t = -1L;
                List<String> external = null;
                for (String s: f.split("\n") )
                {
                    if (s.startsWith("REVISION")) {
                        String[] split = s.split(" ");
                        try {
                            t = ISO8601.toTimeMS(split[4]);
                        } catch (ParseException e) {
                            t = -1L;
                        }
                        revision = s.split(" ")[3];
                    }
                    else if (s.startsWith("MAIN")) {
                        external = new ArrayList<>(Arrays.asList(s.split(" "))).stream().distinct().collect(Collectors.toList());
                                                                                // Converts the list of outlinks to stream, makes it distinct, and converts it back to list
                        external.remove(0);
                    }
                }

            if (!revision.equals("") && external != null && t <= time)
                return new Tuple2<>(revision,new Tuple2<>(t,external));
            return new Tuple2<>("",new Tuple2<>(-1L,new ArrayList<>()));
            }).reduceByKey( ((a,b) -> a._1 > b._1 ? a : b)); // Reduces to the value with maximum timestamp, thus only returning the latest revision.


            System.out.println("RCOUNT: "+records.count());
        // Copies records into ranks and initialize ranks of them to one.
        JavaPairRDD<String, Double> ranks = records.mapValues(rs -> 1.0);
//         PageRank Algorithm from Notes
        for (int current = 0; current < iterations; current++) {
            JavaPairRDD<String, Double> contribs = records.join(ranks).values()
                .flatMapToPair(v -> {
                    List<Tuple2<String, Double>> results = new ArrayList<>();
                    int urlCount = Iterables.size(v._1._2);
                    for (Object s : v._1._2) {
                        results.add(new Tuple2<>(s.toString(), v._2 / urlCount));
                    }
                    return results;
                });
                ranks = contribs.reduceByKey((a, b) -> a + b).mapValues(v -> 0.15 + v * 0.85);
        }
        ranks = ranks.mapToPair( x -> x.swap()).sortByKey(false).mapToPair( x-> x.swap());
        ranks.saveAsTextFile(args[1]);

    }

    private static void usage()
    {
        System.out.println("Usage: PageRank <input> <output> [<iterations> <date>]");
    }
}
