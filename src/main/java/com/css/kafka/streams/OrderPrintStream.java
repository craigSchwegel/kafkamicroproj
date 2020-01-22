package com.css.kafka.streams;

import com.css.kafka.ProducerConsumerCreator;
import com.css.proto.OrderProtos;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import com.css.kafka.IKafkaConstants;
import org.apache.kafka.streams.kstream.KStream;

import java.util.concurrent.CountDownLatch;

public class OrderPrintStream {


    public static void main(String[] args) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        //builder.stream(IKafkaConstants.BYTE_ORDER_TOPIC_NAME).to(IKafkaConstants.TRADE_COUNT_OUTPUT);

        final KStream<Long, byte[]> orders = builder.stream(IKafkaConstants.BYTE_ORDER_TOPIC_NAME);
            orders.foreach((k, barr) ->
                    {
                        try {
                            OrderProtos.OrderMessage ordmsg = OrderProtos.OrderMessage.parseFrom(barr);
                            System.out.println("Stream processing: OrderId = " + ordmsg.getOrderId() +
                                               " Ticker = "+ordmsg.getTicker()+
                                               " Order quantity = " + ordmsg.getQuantity() +
                                               " Order Filled qty = " + ordmsg.getQuantityFilled() +
                                               " Order avg price = " + ordmsg.getAvgPrice());
                        } catch (com.google.protobuf.InvalidProtocolBufferException bufEx) {
                            bufEx.printStackTrace();
                        }
                    }) ;


        final Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, ProducerConsumerCreator.createOrderPrintStreamAppProps());
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("order-print-streams-shutdown-hook") {
            @Override
            public void run()
            {
                System.out.println("What is this thread doing!!!");
                streams.close();
                latch.countDown();
            }
        });

        try
        {
            streams.start();
            latch.await();
        }
        catch (Throwable e)
        {
            System.exit(1);
        }
        System.exit(0);
    }
}
