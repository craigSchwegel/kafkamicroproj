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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class ByteSimulationProducerEngine implements Callable<String> {

    final static Logger logger = LoggerFactory.getLogger(ExecutionSimulatorMain.class);
    private int simulatorId;
    private long transactionLimit;
    private Producer producerSvr;
    private Producer execProdSvr;
    private Producer ordAvgPxSvr;

    public ByteSimulationProducerEngine(int _id, long _nbrTransLimit, Producer _ordProdServer, Producer _execProdServer, Producer _ordAvgPxServer)
    {
        simulatorId = _id;
        transactionLimit = _nbrTransLimit;
        producerSvr = _ordProdServer;
        execProdSvr = _execProdServer;
        ordAvgPxSvr = _ordAvgPxServer;
    }

    @Override
    public String call() throws Exception {

        logger.info("Inside SimulationEngine ID="+ simulatorId +" call() method.");
        logger.debug("Transaction Limit is "+this.transactionLimit);
        long transCount = 0;
        while (transCount++ < this.transactionLimit)
        {
            try{
                OrderProtos.OrderMessage ordMsg = SimulatorHelper.createOrder();
                Long orderId = new Long(ordMsg.getOrderId());
                ProducerRecord<Long, byte[]> record = new ProducerRecord<Long, byte[]>(IKafkaConstants.BYTE_ORDER_TOPIC_NAME, orderId, ordMsg.toByteArray());
                producerSvr.send(record);
                logger.info("SimulationEngine(" + simulatorId + ")::Producing OrderId=" + ordMsg.getOrderId());
                while (ordMsg.getQuantity() > ordMsg.getQuantityFilled())
                {
                    ExecProtos.ExecMessage exeMsg = SimulatorHelper.fillOrder(ordMsg);
                    Long execId = new Long(exeMsg.getExecId());
                    ProducerRecord<Long, byte[]> eRecord = new ProducerRecord<Long, byte[]>(IKafkaConstants.BYTE_EXEC_TOPIC_NAME,execId,exeMsg.toByteArray());
                    execProdSvr.send(eRecord);
                    logger.info("SimulationEngine(" + simulatorId + ")::Writing ExecId=" + exeMsg.getExecId());
                    OrderProtos.OrderMessage.Builder bldr;
                    //calculate average execution price
                    int qtyFilled = ordMsg.getQuantityFilled() + exeMsg.getQuantity();
                    float avgPx = ((ordMsg.getQuantityFilled() * ordMsg.getAvgPrice()) + (exeMsg.getQuantity() * exeMsg.getPrice())) / qtyFilled;
                    bldr = OrderProtos.OrderMessage.newBuilder();
                    bldr.mergeFrom(ordMsg);
                    bldr.setQuantityFilled(qtyFilled);
                    bldr.setAvgPrice(avgPx);
                    ordMsg = bldr.build();
                    record = new ProducerRecord<Long, byte[]>(IKafkaConstants.BYTE_ORDER_AVG_PX_TOPIC_NAME, orderId, ordMsg.toByteArray());
                    ordAvgPxSvr.send(record);
                }
                logger.info("SimulationEngine(" + simulatorId + ")::OrderID=" + ordMsg.getOrderId() + "||Qty=" + ordMsg.getQuantity() + "||QtyFilled=" + ordMsg.getQuantityFilled());

            }
            catch (Exception e)
            {
                logger.error("ERROR:: Simulator="+simulatorId,e);
                logger.error("Breaking out of While loop.  Transaction limit not reached.");
                break;
            }
        } //end while
        logger.info("Exiting Thread for simulationEngine "+simulatorId);
        return "Ending Thread";
    } //end call()

}
