package com.css.tradesimulator;

import com.css.proto.ExecProtos;
import com.css.proto.OrderProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimulationEngine implements Runnable {

    final static Logger logger = LoggerFactory.getLogger(SimulationEngine.class);
    private int simulatorId;
    private long transactionLimit;
    private MsgFileWriterInterface writer;

    public SimulationEngine(int _id, long _nbrTransLimit, MsgFileWriterInterface _writer)
    {
        logger.info("Creating SimulationEngine("+_id+","+_nbrTransLimit+","+_writer.toString()+")");
        simulatorId = _id;
        transactionLimit = _nbrTransLimit;
        writer = _writer;
    }


    @Override
    public void run() {

        logger.info("Inside SimulationEngine ID="+ simulatorId +" run() method");
        long transCount = 0;
        while (transCount++ < this.transactionLimit)
        {
            try{
                OrderProtos.OrderMessage ordMsg = SimulatorHelper.createOrder();
                writer.writeMessage("ORDER:" + ordMsg.toString().replaceAll("\\n", "|"));
                logger.info("SimulationEngine(" + simulatorId + ")::Writing OrderId=" + ordMsg.getOrderId());
                while (ordMsg.getQuantity() > ordMsg.getQuantityFilled()) {
                    ExecProtos.ExecMessage exe = SimulatorHelper.fillOrder(ordMsg);
                    writer.writeMessage("EXEC:" + exe.toString().replaceAll("\\n", "|") + "\\n");
                    logger.info("SimulationEngine(" + simulatorId + ")::Writing ExecId=" + exe.getExecId());
                    ordMsg = ordMsg.toBuilder().setQuantityFilled(ordMsg.getQuantityFilled() + exe.getQuantity()).build();
                }
                logger.info("SimulationEngine(" + simulatorId + ")::OrderID=" + ordMsg.getOrderId() + "||Qty=" + ordMsg.getQuantity() + "||QtyFilled=" + ordMsg.getQuantityFilled());

            }
            catch (Exception e)
            {
               logger.error("ERROR:: Simulator="+simulatorId,e);
               logger.debug("Breaking out of While loop.  Transaction limit not reached.");
               break;
            }
        } //end while
        writer.closeWriter();
        logger.info("SimulationEngine("+ simulatorId +")::Exiting run method.");
    } //end run()

}
