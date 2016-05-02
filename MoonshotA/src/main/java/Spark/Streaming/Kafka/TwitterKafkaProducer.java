package Spark.Streaming.Kafka;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterKafkaProducer {

    private static final String topic = "tweet";
    public static void run(String consumerKey, String consumerSecret,
                           String token, String secret) throws InterruptedException {

        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9094");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("client.id","camus");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
                producerConfig);

        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // add some track terms
        ArrayList<String> arrayList = new ArrayList<String>();
        arrayList.add("twitterapi");
        arrayList.add("a");
        endpoint.trackTerms(arrayList);

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token,
                secret);
        // Authentication auth = new BasicAuth(username, password);

        // Create a new BasicClient. By default gzip is enabled.
        Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                .endpoint(endpoint).authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();

        // Establish a connection
        client.connect();

        // Do whatever needs to be done with messages
        for (int msgRead = 0; msgRead < 1000; msgRead++) {
            KeyedMessage<String, String> message = null;
            try {
                message = new KeyedMessage<String, String>(topic, queue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(message);
            producer.send(message);
        }
        producer.close();
        client.stop();
    }

    public static void main(String[] args) {
        try {
            TwitterKafkaProducer.run("ZiNF7ep9YUImnkoKHZRT8b41s", "vchZaPvjyxf6WRiXObdNY3xjmjLn9rf9O9yHQVtS401RAa4B8t", "563094940-FzdMWCVLhiGASuZu89I8Wy6baWs48tWhAQPuoQNn", "oPDon8VHz6fn6GcjnATnssRhEbsUMZEitJwkXg0lB6ATS");
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }
}