package com.css.kafka;

public interface IKafkaConstants {

    public static String KAFKA_BROKERS = "localhost:9092";
    public static Integer MESSAGE_COUNT=1000;
    public static String ORDER_CLIENT_ID="client1";
    public static String EXEC_CLIENT_ID="client2";
    public static String ORDER_BYTE_CLIENT_ID="client3";
    public static String EXEC_BYTE_CLIENT_ID="client4";
    public static String MEDIAN_CLIENT_ID="median-client1";
    public static String ORDER_CONSUMER_BYTE_CLIENT_ID="client3";
    public static String EXEC_CONSUMER_BYTE_CLIENT_ID="client4";
    public static String ORDER_TOPIC_NAME="orders1";
    public static String BYTE_ORDER_TOPIC_NAME="byte_orders";
    public static String EXEC_TOPIC_NAME="executions";
    public static String BYTE_EXEC_TOPIC_NAME="byte_executions";
    public static String MEDIAN_TOPIC_NAME="median_topic";
    public static String GROUP_ID_CONFIG="consumerGroup1";
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT=100;
    public static String OFFSET_RESET_LATEST="latest";
    public static String OFFSET_RESET_EARLIER="earliest";
    public static Integer MAX_POLL_RECORDS=1;
    public static String CLIENT_ID="client0";
}
