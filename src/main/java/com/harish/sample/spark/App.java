package com.harish.sample.spark;


import com.google.common.collect.Lists;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class App {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local")
                .setAppName("Work Count App 1");
        conf.setExecutorEnv("SPARK_MASTER_HOST", "127.0.0.1");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(5000));


        final Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        kafkaParams.put("zookeeper.connect", "localhost:2181");
        kafkaParams.put("group.id", "123");

        OffsetRange offsetRange[] = new OffsetRange[1];
        offsetRange[0] = OffsetRange.create("com_olacabs_cpp_generic_events", 0, 0l, Long.MAX_VALUE);

        Set<String> topicsSet = new HashSet<String>(Arrays.asList("com_olacabs_cpp_generic_events".split(",")));

        JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(
                sc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );
        JavaDStream<String> lines = kafkaStream.map(new FirstMap());
        JavaDStream<String> words = lines.flatMap(new FlatMapper());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new XX()).reduceByKey(new XY());
        wordCounts.print();

        sc.start();
        sc.awaitTermination();
    }

    public static void wordCountJava7(String filename) {
        // Define a configuration to use to interact with Spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");

        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the input data, which is a text file read from the command line
        JavaRDD<String> input = sc.textFile(filename);

        // Java 7 and earlier
        JavaRDD<String> words = input.flatMap(new FlatMapper());

        // Java 7 and earlier: transform the collection of words into pairs (word and 1)
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2(s, 1);
                    }
                });

        // Java 7 and earlier: count the words
        JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer x, Integer y) {
                        return x + y;
                    }
                });

        // Save the word count back out to a text file, causing evaluation.
        reducedCounts.saveAsTextFile("output");
    }


}

class FirstMap implements Function<Tuple2<String, String>, String>, Serializable {
    @Override
    public String call(Tuple2<String, String> tuple2) throws Exception {
        return tuple2._2();
    }
}

class FlatMapper implements FlatMapFunction<String, String> {
    private static final Pattern SPACE = Pattern.compile(" ");
    @Override
    public Iterator<String> call(String s) throws Exception {
        return Lists.newArrayList(SPACE.split(s)).iterator();
    }
}

/*
class FlatMapper_<T extends String, R extends String> implements FlatMapFunction<T, R> {


    @Override
    public Iterable<R> call(T s) {
        ArrayList<R> sa = new ArrayList<>();
        if (Strings.isNullOrEmpty((String) s)) {
        }
        for( String s1 : s.split(" ")) {
            sa.add((R)s1);
        }
        // return Lists.newArrayList(SPACE.split(s));
        return sa;
    }
}
*/

class XX1 implements PairFunction<ArrayList<String>, String, Integer>, Serializable {

    @Override
    public Tuple2<String, Integer> call(ArrayList<String> strings) throws Exception {
        return null;
    }
}

class XX implements PairFunction<String, String, Integer>, Serializable {

    @Override
    public Tuple2<String, Integer> call(String s) throws Exception {
        return new Tuple2<String, Integer>(s, 1);
    }
}

class XY implements Function2<Integer, Integer, Integer> {

    @Override
    public Integer call(Integer integer, Integer integer2) throws Exception {
        return integer + integer2;
    }
}