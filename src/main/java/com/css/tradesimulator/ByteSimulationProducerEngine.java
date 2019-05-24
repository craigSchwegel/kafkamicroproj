/*
 * ByteSimulationProducerEngine is a stand alone service that simulates trading of stocks
 * by creating Orders and then randomly filling the Orders by creating Executions of various
 * quantities until the Order is completely filled.
 * Technical: The service expects the caller to pass in Kafka Producer for the Order and
 * Execution topics.  Orders and Executions objects are created using Google Protocol Buffers
 * version 3. The Orders and Executions are serialized as byte[] before writing to the topic.
 * Constructor Parameters
 * simulatorId - unique identifier for this process used for debugging in log files
 * transactionLimit - upper limit on number of Orders created
 * producerServer - Kafka client Producer server used for writing Orders
 * execProdServer - Kafka client Producer server used for writing Executions
 *
 * @author  Craig Schwegel
 * @version 1.0
 * @since   2019-05-24
 */

package com.css.tradesimulator;

import com.css.proto.ExecProtos;
import com.css.proto.OrderProtos;
import com.css.kafka.IKafkaConstants;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.Callable;

public class ByteSimulationProducerEngine implements Callable<String> {

    private int simulatorId;
    private long transactionLimit;
    private Producer producerSvr;
    private Producer execProdSvr;

    public ByteSimulationProducerEngine(int _id, long _nbrTransLimit, Producer _ordProdServer, Producer _execProdServer)
    {
        simulatorId = _id;
        transactionLimit = _nbrTransLimit;
        producerSvr = _ordProdServer;
        execProdSvr = _execProdServer;
    }

    @Override
    public String call() throws Exception {

        System.out.println("Inside SimulationEngine ID="+ simulatorId +" run() method");
        long transCount = 0;
        while (transCount++ < this.transactionLimit)
        {
            try{
                OrderProtos.OrderMessage ordMsg = SimulatorHelper.createOrder();
                Long orderId = new Long(ordMsg.getOrderId());
                ProducerRecord<Long, byte[]> record = new ProducerRecord<Long, byte[]>(IKafkaConstants.BYTE_ORDER_TOPIC_NAME, orderId, ordMsg.toByteArray());
                producerSvr.send(record);
                System.out.println("SimulationEngine(" + simulatorId + ")::Producing OrderId=" + ordMsg.getOrderId());
                while (ordMsg.getQuantity() > ordMsg.getQuantityFilled())
                {
                    ExecProtos.ExecMessage exeMsg = SimulatorHelper.fillOrder(ordMsg);
                    Long execId = new Long(exeMsg.getExecId());
                    ProducerRecord<Long, byte[]> eRecord = new ProducerRecord<Long, byte[]>(IKafkaConstants.BYTE_EXEC_TOPIC_NAME,execId,exeMsg.toByteArray());
                    execProdSvr.send(eRecord);
                    System.out.println("SimulationEngine(" + simulatorId + ")::Writing ExecId=" + exeMsg.getExecId());
                    ordMsg = ordMsg.toBuilder().setQuantityFilled(ordMsg.getQuantityFilled() + exeMsg.getQuantity()).build();
                }
                System.out.println("SimulationEngine(" + simulatorId + ")::OrderID=" + ordMsg.getOrderId() + "||Qty=" + ordMsg.getQuantity() + "||QtyFilled=" + ordMsg.getQuantityFilled());

            }
            catch (Exception e)
            {
                System.out.println("ERROR:: Simulator="+simulatorId);
                System.out.println("Breaking out of While loop.  Transaction limit not reached.");
                break;
            }
        } //end while
        return "Ending Thread";
    } //end call()

}
