package Spark.Streaming.Kafka;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import javax.swing.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.util.*;



public class KafkaStreaming {

    private static JavaSparkContext sc;
    private static Map<String,Integer> WordCountMap = new HashMap();
    private static JFrame f = new JFrame();


    private static final class StreamMessages implements Function<Tuple2<String,String>, String> {
        public String call(Tuple2<String, String> message) {
            String jsonl = "";
            jsonl = message._2.toString();
            char c = jsonl.charAt(0);
            String result = "";
            if(c == '{') {
                JsonParser parser = new JsonParser();
                JsonObject rootObj = parser.parse(jsonl).getAsJsonObject();
                if (rootObj.get("text") != null) {
                    result = rootObj.get("text").toString();
                    String[] arr =result.split(" ");
                    for ( String ss : arr) {
                        if(WordCountMap.containsKey(ss))
                            WordCountMap.put(ss, WordCountMap.get(ss)+1);
                        else
                            WordCountMap.put(ss, 0);
                    }
                }
            }

            if(WordCountMap!=null)
            {
                f.setSize(3000, 1000);
                double[] values = new double[WordCountMap.keySet().size()];
                String[] names = new String[WordCountMap.keySet().size()];
                int IndexG = 0;
                for (String key : WordCountMap.keySet()) {
                        values[IndexG] = WordCountMap.get(key);
                        names[IndexG] = key;
                        IndexG++;
                }

                if(names.length > 0 && names.length<50)
                {
                    f.getContentPane().add(new ChartPanel(values, names, "Word Count"));

                    WindowListener wndCloser = new WindowAdapter() {
                        public void windowClosing(WindowEvent e) {
                            System.exit(0);
                        }
                    };
                    values = null;
                    names = null;
                    f.addWindowListener(wndCloser);
                    f.setVisible(true);
                }
            }
            return result;
        }
    };

    private static final class splitWords implements FlatMapFunction<String, String> {
        public Iterable<String> call(String x) {
            return Arrays.asList(x.split(" "));
        }
    };

    private static final class wordMapper implements PairFunction<String, String, Integer> {
        public Tuple2<String, Integer> call(String x) {
            return new Tuple2(x, 1);
        }
    };

    private static final class WordCountReducer implements Function2<Integer, Integer, Integer> {
        public Integer call(Integer x, Integer y) {
            return x + y;
        }
    };


    public KafkaStreaming(String sparkAppName, List<String> topicList, int numberThreads) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName(sparkAppName).setMaster("local[4]");
        sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, new Duration(3000));
        sparkStreaming(topicList, numberThreads, jsc);
    }

    private void sparkStreaming(List<String> topicList, int numberThreads, JavaStreamingContext jsc) {
        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic : topicList) {
            topicMap.put(topic, numberThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jsc, "localhost:2181", "tweetg", topicMap);

        JavaDStream<String> line = messages.map(new StreamMessages());

        JavaDStream<String> words = line.flatMap(new splitWords());

        JavaPairDStream<String, Integer> wordCount = words.mapToPair(new wordMapper()).reduceByKey(new WordCountReducer());

        sc = jsc.sparkContext();
        wordCount.print();
        jsc.start();
        jsc.awaitTermination();
    }
}
