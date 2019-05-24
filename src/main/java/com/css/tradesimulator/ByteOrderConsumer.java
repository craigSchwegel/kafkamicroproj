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

import com.css.proto.OrderProtos;
import com.css.kafka.ProduerConsumerCreator;
import com.css.kafka.IKafkaConstants;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Arrays;

public class ByteOrderConsumer {

    public static void main(String[] args) {
        System.out.println("ByteOrderConsumer::main() Starting Consumer...");
        runConsumer();
    }
    static void runConsumer() {
        Consumer<Long, byte[]> consumer = ProduerConsumerCreator.createByteOrderConsumer();
        int noMessageFound = 0;
        consumer.subscribe(Arrays.asList(IKafkaConstants.BYTE_ORDER_TOPIC_NAME));
        try {
            while (true) {
                ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Long, byte[]> record : records) {
                    OrderProtos.OrderMessage ordMsg;
                    try
                    {
                        ordMsg = OrderProtos.OrderMessage.parseFrom(record.value());
                        System.out.printf("offset = %d, key = %d, value = %s%n", record.offset(), record.key(), ordMsg.toString());
                    }
                    catch (com.google.protobuf.InvalidProtocolBufferException ex)
                    {
                        System.out.println("ByteOrderConsumer::ERROR parsing Order from bytes[]");
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}
