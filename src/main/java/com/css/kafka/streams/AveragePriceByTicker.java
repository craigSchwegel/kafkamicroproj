package com.css.kafka.streams;

import com.css.kafka.IKafkaConstants;
import com.css.kafka.IProtoHelper;
import com.css.kafka.ProducerConsumerCreator;
import com.css.proto.AvgPxByTickerProtos;
import com.css.proto.OrderProtos;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.concurrent.CountDownLatch;

public class AveragePriceByTicker {


    public static void main(String[] args) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        //builder.stream(IKafkaConstants.BYTE_ORDER_TOPIC_NAME).to(IKafkaConstants.TRADE_COUNT_OUTPUT);

        final KStream<Long, byte[]> orders = builder.stream(IKafkaConstants.BYTE_ORDER_AVG_PX_TOPIC_NAME);
        final KStream<Long, byte[]> execs = builder.stream(IKafkaConstants.BYTE_EXEC_TOPIC_NAME);

        /** Java 7 version **
        KStream<String, byte[]> ordAvgPxStreamJavaV7 = orders.map( new KeyValueMapper<Long, byte[], KeyValue<String, byte[]>> ()
        {
            @Override
            public KeyValue<String, byte[]> apply(Long key, byte[] value){
                OrderProtos.OrderMessage ordMsg = IProtoHelper.orderMessageConverter(value);
                return new KeyValue<String, byte[]>(ordMsg.getTicker(), ordMsg.toByteArray());
            }
        });
        **/
        KStream<String, byte[]> ordAvgPxStreamJavaV8 = orders.map( (key, value) -> {
            OrderProtos.OrderMessage ordMsg = IProtoHelper.orderMessageConverter(value);
            return KeyValue.pair(ordMsg.getTicker(),ordMsg.toByteArray());
        });

        /**                                                                            }
        GlobalKTable<String,byte[]> orderAvgPxTable = builder.globalTable(IKafkaConstants.BYTE_ORDER_AVG_PX_TOPIC_NAME);
        GlobalKTable<Long,byte[]> orderTable = builder.globalTable(IKafkaConstants.BYTE_ORDER_TOPIC_NAME);
        GlobalKTable<Long,byte[]> execTable = builder.globalTable(IKafkaConstants.BYTE_EXEC_TOPIC_NAME);
        GlobalKTable<String,byte[]> avgPxByTicker = builder.globalTable(IKafkaConstants.AVG_PRICE_BY_TICKER_TOPIC_NAME,
                                                            Materialized.<String, byte[], KeyValueStore<Bytes, byte[]>>as("order-exec-joined-store")
                                                                    .withKeySerde(Serdes.String())
                                                                .withValueSerde(Serdes.ByteArray()));
**/
        //GlobalKTable<Long,byte[]> orderAvgPxTableMaterialized = builder.globalTable(IKafkaConstants.BYTE_ORDER_AVG_PX_TOPIC_NAME,"my-table", Materialized<Long, byte[], KeyValueStore<Byte, byte[]> >);

            //OrderProtos.OrderMessage ordMsg = IProtoHelper.orderMessageConverter(newVal);
            //return KeyValue.pair(ordMsg.getTicker(),ordMsg.toByteArray());
            //return ordMsg.toByteArray();
            //return newVal;


        //TODO: Add OrderValueExtractor(byte[])
        //TODO: Add OrderObjectExtractor(byte[])
        //To encapsulate the conversion from byte[] to Order object

        //orders.join(execs, (k, v) -> { k.})
        /**
        KTable<String, String> averagePricesByTicker = orders.map( (k, barr) -> {
            OrderProtos.OrderMessage ordMsg = null;
            String ticker = "";
            String data = "";
            try {
                ordMsg = OrderProtos.OrderMessage.parseFrom(barr);
                ticker = ordMsg.getTicker();
                data = ordMsg.getTicker()+"|"+ordMsg.getAvgPrice()+"|"+ordMsg.getQuantityFilled();
            }
            catch (com.google.protobuf.InvalidProtocolBufferException bufEx) {
                bufEx.printStackTrace();
            }

             return KeyValue.pair(ticker, data);
        }).groupByKey();
         **/
        orders.map( (k, barr) ->
        {
            OrderProtos.OrderMessage ordmsg = null;
            String data = "";
            try {
                ordmsg = OrderProtos.OrderMessage.parseFrom(barr);
                data = ordmsg.getTicker() +"|"+ ordmsg.getAvgPrice();
            } catch (com.google.protobuf.InvalidProtocolBufferException bufEx) {
                bufEx.printStackTrace();
            }
            return KeyValue.pair(k.toString(), data);
        }).foreach( (k, barData) -> System.out.println(k+"="+barData));
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

            //final KStream<String, String> textLines = builder.stream(inputTopic);

        final Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, ProducerConsumerCreator.createAveragePriceByTickerStreamAppProps());
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("tradecount-streams-shutdown-hook") {
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
