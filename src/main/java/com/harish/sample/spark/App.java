package com.harish.sample.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.NGram;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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
import scala.collection.mutable.WrappedArray;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.spark.ml.feature.Tokenizer;

public class App {
    private static final Pattern SPACE = Pattern.compile(" ");
    public static int counter;

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local")
                .setAppName("Work Count App 1");

        if (true) {
            // new JavaNGramExample().main(args);
            // return;

            Tokenizer(conf);
            return;
        }



        conf.setExecutorEnv("SPARK_MASTER_HOST", "127.0.0.1");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(5000));


        final Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        kafkaParams.put("zookeeper.connect", "localhost:2181");
        kafkaParams.put("group.id", "123");
        kafkaParams.put("startingOffsets", "earliest");

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
        // JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new XX()).reduceByKey(new XY());


        //wordCounts.print();
        lines.foreachRDD((stringJavaRDD, time) -> {
            SparkSession spark = JavaSparkSessionSingleton.getInstance(stringJavaRDD.context().getConf());

            JavaRDD<org.apache.spark.sql.Row> rowRDD = stringJavaRDD.map(new Function<String, Row>() {
                @Override
                public Row call(String msg) {
                    String s[] = new String[2];
                    s[0] = msg;
                    s[1] = "The";
                    if (msg.split(" ") == null || msg.split(" ").length == 0)
                    {
                        return null;
                    }
                    return RowFactory.create(counter++, msg.split(" "));
                }
            });

            StructType schema = new StructType(new StructField[]{
                    new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                    new StructField("words",  DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
            });

            Dataset<org.apache.spark.sql.Row>  msgDataFrame = spark.createDataFrame(rowRDD, schema);
            msgDataFrame.show();

            NGram ngramTransformer = new NGram().setN(10).setInputCol("words").setOutputCol("ngrams");

            Dataset<org.apache.spark.sql.Row> ngramDataFrame = ngramTransformer.transform(msgDataFrame);
            ngramDataFrame.select("*").show(false);
        });


        sc.start();
        sc.awaitTermination();
    }

    public static void CorrelationExample(SparkConf conf) {
        SparkSession spark = JavaSparkSessionSingleton.getInstance(conf);

        List<Row> data = Arrays.asList(
                RowFactory.create(Vectors.sparse(4, new int[]{0, 3}, new double[]{1.0, -2.0})),
                RowFactory.create(Vectors.dense(4.0, 5.0, 0.0, 3.0)),
                RowFactory.create(Vectors.dense(6.0, 7.0, 0.0, 8.0)),
                RowFactory.create(Vectors.sparse(4, new int[]{0, 3}, new double[]{9.0, 1.0}))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);
        Row r1 = Correlation.corr(df, "features").head();
        System.out.println("Pearson correlation matrix:\n" + r1.get(0).toString());

        Row r2 = Correlation.corr(df, "features", "spearman").head();
        System.out.println("Spearman correlation matrix:\n" + r2.get(0).toString());
    }

    public static void Tokenizer(SparkConf conf) {
        SparkSession spark = JavaSparkSessionSingleton.getInstance(conf);

        List<Row> data = Arrays.asList(
                RowFactory.create(0, "Hi I heard about Spark"),
                RowFactory.create(1, "I wish Java could use case classes"),
                RowFactory.create(2, "Logistic,regression,models,are,neat")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> sentenceDataFrame = spark.createDataFrame(data, schema);

        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");

        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol("sentence")
                .setOutputCol("words")
                .setPattern("\\W");  // alternatively .setPattern("\\w+").setGaps(false);

        spark.udf().register(
                "countTokens", (WrappedArray<?> words) -> words.size(), DataTypes.IntegerType);

        Dataset<Row> tokenized = tokenizer.transform(sentenceDataFrame);
        tokenized.select("sentence", "words")
                .withColumn("tokens", callUDF("countTokens", col("words")))
                .show(false);
/*
+-----------------------------------+------------------------------------------+------+
|sentence                           |words                                     |tokens|
+-----------------------------------+------------------------------------------+------+
|Hi I heard about Spark             |[hi, i, heard, about, spark]              |5     |
|I wish Java could use case classes |[i, wish, java, could, use, case, classes]|7     |
|Logistic,regression,models,are,neat|[logistic,regression,models,are,neat]     |1     |
+-----------------------------------+------------------------------------------+------+
*/

        Dataset<Row> regexTokenized = regexTokenizer.transform(sentenceDataFrame);
        regexTokenized.select("sentence", "words")
                .withColumn("tokens", callUDF("countTokens", col("words")))
                .show(false);
/*
+-----------------------------------+------------------------------------------+------+
|sentence                           |words                                     |tokens|
+-----------------------------------+------------------------------------------+------+
|Hi I heard about Spark             |[hi, i, heard, about, spark]              |5     |
|I wish Java could use case classes |[i, wish, java, could, use, case, classes]|7     |
|Logistic,regression,models,are,neat|[logistic, regression, models, are, neat] |5     |
+-----------------------------------+------------------------------------------+------+
 */

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


class JavaNGramExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("JavaNGramExample")
                .getOrCreate();

        // $example on$
        List<org.apache.spark.sql.Row> data = Arrays.asList(
                RowFactory.create(0, Arrays.asList("Hi", "I", "heard", "about", "Spark")),
                RowFactory.create(1, Arrays.asList("I", "wish", "Java", "could", "use", "case", "classes")),
                RowFactory.create(2, Arrays.asList("Logistic", "regression", "models", "are", "neat"))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField(
                        "words", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
        });

        Dataset<org.apache.spark.sql.Row> wordDataFrame = spark.createDataFrame(data, schema);

        NGram ngramTransformer = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams");

        Dataset<org.apache.spark.sql.Row> ngramDataFrame = ngramTransformer.transform(wordDataFrame);
        ngramDataFrame.select("ngrams").show(false);
        // $example off$

        spark.stop();
    }
}

class JavaSparkSessionSingleton {
    private static transient SparkSession instance = null;
    public static SparkSession getInstance(SparkConf sparkConf) {
        if (instance == null) {
            instance = SparkSession
                    .builder()
                    .config(sparkConf)
                    .getOrCreate();
        }
        return instance;
    }
}