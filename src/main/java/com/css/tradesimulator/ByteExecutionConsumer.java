/*
 * ByteExecutionConsumer is a stand alone service that consumes Executions created by the
 * TradeSimulationMain services and prints the content.
 * Technical: The service creates a Kafka Consumer client for the Execution topic.  When an
 * Execution is received, the service deserializes the object from byte[] into Google Protocol
 * Buffer v3 and prints the content.
 *
 * @author  Craig Schwegel
 * @version 1.0
 * @since   2019-05-24
 */
package com.css.tradesimulator;

import com.css.kafka.ProducerConsumerCreator;
import com.css.proto.ExecProtos;
import com.css.kafka.IKafkaConstants;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

public class ByteExecutionConsumer {

    final static Logger logger = LoggerFactory.getLogger(ByteExecutionConsumer.class);
    public static void main(String[] args) {
        logger.info("main() Starting Consumer...");
        runConsumer();
    }
    static void runConsumer() {
        logger.info("Creating ByteExecutionConsumer...");
        Consumer<Long, byte[]> consumer = ProducerConsumerCreator.createByteExecutionConsumer();
        int noMessageFound = 0;
        logger.info("Subscribing to "+IKafkaConstants.BYTE_EXEC_TOPIC_NAME);
        consumer.subscribe(Arrays.asList(IKafkaConstants.BYTE_EXEC_TOPIC_NAME));
        try {
            while (true) {
                ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Long, byte[]> record : records) {
                    ExecProtos.ExecMessage execMsg;
                    try
                    {
                        execMsg = ExecProtos.ExecMessage.parseFrom(record.value());
                        logger.info("offset = %d, key = %d, value = %s%n", record.offset(), record.key(), execMsg.toString());
                    }
                    catch (com.google.protobuf.InvalidProtocolBufferException ex)
                    {
                        logger.error("Caught exception parsing Execution from bytes[]",ex);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Caught exception consuming from topic "+IKafkaConstants.BYTE_EXEC_TOPIC_NAME,e);
        } finally {
            if (consumer != null)
                consumer.close();
        }
    }
}
