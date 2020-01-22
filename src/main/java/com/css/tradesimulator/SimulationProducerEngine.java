/*
 * SimulationProducerEngine is a stand alone service that simulates trading of stocks
 * by creating Orders and then randomly filling the Orders by creating Executions of various
 * quantities until the Order is completely filled.
 * Technical: The service expects the caller to pass in Kafka Producer for the Order and
 * Execution topics.  Orders and Executions objects are created using Google Protocol Buffers
 * version 3. The Orders and Executions are serialized as String before writing to the topic.
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

public class SimulationProducerEngine implements Callable<String> {

    final static Logger logger = LoggerFactory.getLogger(SimulationProducerEngine.class);
    private int simulatorId;
    private long transactionLimit;
    private Producer producerSvr;
    private Producer execProdSvr;

    public SimulationProducerEngine(int _id, long _nbrTransLimit, Producer _ordProdServer, Producer _execProdServer)
    {
        simulatorId = _id;
        transactionLimit = _nbrTransLimit;
        producerSvr = _ordProdServer;
        execProdSvr = _execProdServer;
    }

    @Override
    public String call() throws Exception {

        logger.info("Begin SimulationEngine ID="+ simulatorId +" call() method");
        long transCount = 0;
        while (transCount++ < this.transactionLimit)
        {
            try{
                OrderProtos.OrderMessage ordMsg = SimulatorHelper.createOrder();
                //producerSvr
                Long orderId = Long.getLong(Integer.toString(ordMsg.getOrderId()));
                ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.ORDER_TOPIC_NAME, orderId, ordMsg.toString().replaceAll("\\n", "|"));
                producerSvr.send(record);
                logger.info("SimulationEngine(" + simulatorId + ")::Producing OrderId=" + ordMsg.getOrderId());
                while (ordMsg.getQuantity() > ordMsg.getQuantityFilled())
                {
                    ExecProtos.ExecMessage exeMsg = SimulatorHelper.fillOrder(ordMsg);
                    Long execId = Long.getLong(Integer.toString(exeMsg.getExecId()));
                    ProducerRecord<Long, String> eRecord = new ProducerRecord<Long, String>(IKafkaConstants.EXEC_TOPIC_NAME,execId,exeMsg.toString().replaceAll("\\n", "|"));
                    execProdSvr.send(eRecord);
                    logger.info("SimulationEngine(" + simulatorId + ")::Writing ExecId=" + exeMsg.getExecId());
                    ordMsg = ordMsg.toBuilder().setQuantityFilled(ordMsg.getQuantityFilled() + exeMsg.getQuantity()).build();
                }
                logger.info("SimulationEngine(" + simulatorId + ")::OrderID=" + ordMsg.getOrderId() + "||Qty=" + ordMsg.getQuantity() + "||QtyFilled=" + ordMsg.getQuantityFilled());

            }
            catch (Exception e)
            {
                logger.error("SimulatorEngine("+simulatorId+")::"+e.getMessage());
                logger.error("SimulatorEngine("+simulatorId+")",e);
                logger.debug("Breaking out of While loop.  Transaction limit not reached.");
                break;
            }
        } //end while
        logger.info("SimulationEngine(" + simulatorId + ")::Ending Thread");
        return "Ending Thread";
    } //end call()

}
