package com.css.tradesimulator;

import com.css.proto.ExecProtos;
import com.css.proto.OrderProtos;

public class SimulationEngine implements Runnable {

    private int simulatorId;
    private long transactionLimit;
    private MsgFileWriterInterface writer;

    public SimulationEngine(int _id, long _nbrTransLimit, MsgFileWriterInterface _writer)
    {
        simulatorId = _id;
        transactionLimit = _nbrTransLimit;
        writer = _writer;
    }


    @Override
    public void run() {

        System.out.println("Inside SimulationEngine ID="+ simulatorId +" run() method");
        long transCount = 0;
        while (transCount++ < this.transactionLimit)
        {
            try{
                OrderProtos.OrderMessage ordMsg = SimulatorHelper.createOrder();
                writer.writeMessage("ORDER:" + ordMsg.toString().replaceAll("\\n", "|"));
                System.out.println("SimulationEngine(" + simulatorId + ")::Writing OrderId=" + ordMsg.getOrderId());
                while (ordMsg.getQuantity() > ordMsg.getQuantityFilled()) {
                    ExecProtos.ExecMessage exe = SimulatorHelper.fillOrder(ordMsg);
                    writer.writeMessage("EXEC:" + exe.toString().replaceAll("\\n", "|") + "\\n");
                    System.out.println("SimulationEngine(" + simulatorId + ")::Writing ExecId=" + exe.getExecId());
                    ordMsg = ordMsg.toBuilder().setQuantityFilled(ordMsg.getQuantityFilled() + exe.getQuantity()).build();
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
        writer.closeWriter();
    } //end run()

}
