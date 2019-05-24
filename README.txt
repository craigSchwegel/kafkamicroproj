Trade Simulator and Analytics Engine (Microservice architecture)

The kafkamicroproj is a proof of concept with the goal of using microservices
architecture to build a trade analytics platform. The project is developed with
microservice components starting with a trade simulator that models trades on 
stocks as Orders and Executions.  The platform then has other microservices that 
listen to the Orders and Executions to generate analytics that can be used to make
trading decisions in real time. The output is a statistical characteristic that
gives the trader the Median quantity executed for a stock ticker.  Median is preferred
over average as it is not affected by outliers.  This architecture makes it very easy
to build out other analytic characteristics using the same input but as separate services
which is more scalable and can run independendly.  

This project is architected using 
Apache Kafka 2.2 
Java 1.8
Google Protocol Buffers 3.7.

Technical Details
package: com.css.tradesimulator
TradeSimulationMain - Main class for creating the simulator services that will create 
Orders and fill them with Executions

ByteSimulationProducerEngine - Creates Orders as protobuf objects and writes them to 
topics serialized as byte[].  This service generates Executions as protobuf objects
randomly until the Order is completley filled.  Executions are written as byte[]. Keys 
for all topics are Long type.

ByteOrderConsumer - Service that listens to Byte Order topic and prints the details when 
received.  The service deserializes the byte[] into a protobuf object before printing.

ByteExecutionConsumer - Service that listens to Byte Execution topic and prints the details when 
received.  The service deserializes the byte[] into a protobuf object before printing.

package: com.css.micro.median
MedianConsumer - starting point for the microservice that listens to Orders and Executions and
calculates the median execution quantity for a ticker.  The service outputs the updated median 
per ticker each time it is updated in format ticker|median to the median topic.

StreamingMedian - This class takes an integer as input, stores it in a sorted custom linked list
and calculates the median after reciving each input.  The algorithm runs in O(n) runtime complexity
which is very efficient as the size of n inouts grows.  It is able to achieve this runtime as the 
list is sorted while inserting and median node is kept track using a reference that is moved forward
or backward after each insert.

Instructions for running the platform
// Download Apache Kafka 2.2
https://kafka.apache.org/downloads

// Download GoogleProtocol Buffers
https://developers.google.com/protocol-buffers/docs/downloads

// start the Kafka cluster
cd to kafka home directory - C:\projects\develop\kafka-2.2.0-src

// start zookeeper
bin\windows\zookeeper-server-start config\zookeeper.properties

// start Kafka broker
bin\windows\kafka-server-start config/server.properties

// start Consumers
java -cp C:\projects\repo\kafkamicroproj\target\KafkaMicroTradeAnalyics-1.0-jar-with-dependencies.jar com.css.tradesimulator.ByteOrderConsumer
java -cp C:\projects\repo\kafkamicroproj\target\KafkaMicroTradeAnalyics-1.0-jar-with-dependencies.jar com.css.tradesimulator.ByteExecutionConsumer
java -cp C:\projects\repo\kafkamicroproj\target\KafkaMicroTradeAnalyics-1.0-jar-with-dependencies.jar com.css.micro.median.MedianConsumer

//start trade simulator and confirm messages are being consumed by Order and Execution consumers
java -cp C:\projects\repo\kafkamicroproj\target\KafkaMicroTradeAnalyics-1.0-jar-with-dependencies.jar com.css.tradesimulator.TradeSimulationMain



