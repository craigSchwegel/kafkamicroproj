/*
 * ByteOrderConsumer is a stand alone service that consumes Orders created by the
 * TradeSimulationMain services and prints the content.
 * Technical: The service creates a Kafka Consumer client for the byte order topic.  When an
 * Order is received, the service deserializes the object from byte[] into Google Protocol
 * Buffer v3 and prints the content.
 *
 * @author  Craig Schwegel
 * @version 1.0
 * @since   2019-05-24
 */
package com.css.tradesimulator;

import com.css.kafka.ProducerConsumerCreator;
import com.css.proto.OrderProtos;
import com.css.kafka.IKafkaConstants;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

public class ByteOrderConsumer {

    final static Logger logger = LoggerFactory.getLogger(ByteOrderConsumer.class);
    public static void main(String[] args) {
        logger.info("ByteOrderConsumer::main() Starting Consumer...");
        runConsumer();
    }
    static void runConsumer() {
        logger.info("Creating ByteOrderConsumer...");
        Consumer<Long, byte[]> consumer = ProducerConsumerCreator.createByteOrderConsumer();
        int noMessageFound = 0;
        logger.info("Subscribing to "+IKafkaConstants.BYTE_ORDER_TOPIC_NAME);
        consumer.subscribe(Arrays.asList(IKafkaConstants.BYTE_ORDER_TOPIC_NAME));
        try {
            while (true) {
                ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Long, byte[]> record : records) {
                    OrderProtos.OrderMessage ordMsg;
                    try
                    {
                        ordMsg = OrderProtos.OrderMessage.parseFrom(record.value());
                        logger.info("offset = %d, key = %d, value = %s%n", record.offset(), record.key(), ordMsg.toString());
                    }
                    catch (com.google.protobuf.InvalidProtocolBufferException ex)
                    {
                        logger.error("ByteOrderConsumer::ERROR parsing Order from bytes[]",ex);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Caught exception consuming from topic "+IKafkaConstants.BYTE_ORDER_TOPIC_NAME,e);
        } finally {
            if (consumer != null)
                consumer.close();
        }
        logger.info("Leaving runConsumer()...");
    }
}
