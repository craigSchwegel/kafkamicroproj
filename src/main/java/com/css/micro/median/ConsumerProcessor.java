/*
 * ConsumerProcessor is a generic class for consuming messages off a Kafka
 * topic and writing them to BlockingQueue.  The purpose of this class is to
 * move the processing of the Kafka messages off of the Kafka thread and onto
 * a worker thread with in the service.  This is a typical Producer / Consumer
 * pattern which is thread safe through the use of the Blocking Queue.
 *
 * @author  Craig Schwegel
 * @version 1.0
 * @since   2019-05-24
 */
package com.css.micro.median;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

public class ConsumerProcessor implements Callable<String> {

    final static Logger logger = LoggerFactory.getLogger(ConsumerProcessor.class);
    public ConsumerProcessor(BlockingQueue<byte[]> queueVal, Consumer<Long, byte[]> consumerVal, String topicVal)
    {
        queue = queueVal;
        consumer = consumerVal;
        topic = topicVal;
    }

    private BlockingQueue<byte[]> queue;
    private Consumer<Long, byte[]> consumer;
    private String topic;

    @Override
    public String call() throws Exception {
        logger.info("Begin call() method");
        consumer.subscribe(Arrays.asList(topic));
        try {
            while (true) {
                ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Long, byte[]> record : records) {
                    queue.put(record.value());
                }
            }
        } catch (Exception e) {
            logger.error("Caught exception consuming records.",e);
        } finally {
            if (consumer != null)
                consumer.close();
            return "Completed";
        }
    }


}
