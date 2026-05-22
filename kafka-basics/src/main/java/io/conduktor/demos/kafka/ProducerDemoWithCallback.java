package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args)
    {
        log.info("Starting Producer...");

        // create Producer Properties
        Properties properties = new Properties();

        // connect to Localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect to Conduktor Playground
//        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
//        properties.setProperty("security.protocol", "SASL_SSL");
//        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"your-username\" password=\"your-password\";");
//        properties.setProperty("sasl.mechanism", "PLAIN");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // not recommended in prod, default is 16384
         properties.setProperty("batch.size", "400");

        // not recommended in prod
        /* default is based on key
         * 1. If the record has a key
         * same key -> same partition
         * 2. If the record has no key
         * Kafka uses the default sticky partitioning behavior:
         * send multiple records to one partition for a while,
         * then switch to another partition
         * */
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // produce 300 records
        for (int i = 0; i < 300; i++){
            // create a Producer Record
            final int messageNumber = i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("demo_java", "hello world: " + messageNumber);

            // send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes every time a record successfully sent or an exception is thrown
                    if (exception == null){
                        // the record was successfully sent
                        log.info("Message {} -> partition {} offset {}", messageNumber, metadata.partition(), metadata.offset());
                    }
                    else {
                        log.error("Error while producing", exception);
                    }
                }
            });
        }


        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // tell the producer to send all data and block until done - synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
