package com.css.tradesimulator;

import com.css.proto.ExecProtos;
import com.css.proto.OrderProtos;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;

public class SimulatorHelper {

    private static volatile int ordId = 100;
    private static volatile int execId = 100;
    private final static Object createOrderMutex = new Object();
    public static OrderProtos.OrderMessage createOrder()
    {
        synchronized (createOrderMutex) {
            OrderProtos.OrderMessage ordMsg;
            OrderProtos.OrderMessage.Builder bldr;
            bldr = OrderProtos.OrderMessage.newBuilder();

            int iQty = (int) ((Math.random() * 99999) % 10000);
            if (iQty == 0)
                iQty = 3500;
            bldr.setQuantity(iQty);
            bldr.setOrderId(SimulatorHelper.getNextOrderId());
            bldr.setQuantityFilled(0);
            bldr.setSide(OrderProtos.OrderMessage.BuySell.BUY);
            bldr.setAuditTime(Instant.now().getEpochSecond());
            bldr.setAuditUser("CTrader");
            bldr.setAvgPrice(0.0f);
            bldr.setCustomer("CITI");
            bldr.setLegalEntity("JPM");
            bldr.setTradeAccount("DERIVATIVE.HEDGE.ACCT");
            bldr.setOrderStatus("NEW");
            int iTickerChoice = (int) (Math.random() % 3);
            switch (iTickerChoice) {
                case 0:
                    bldr.setTicker("MSFT");
                    break;
                case 1:
                    bldr.setTicker("FB");
                    break;
                case 2:
                    bldr.setTicker("GM");
                    break;
                default:
                    bldr.setTicker("AMZN");
            }

            return bldr.build();
        }
    }

    private final static Object fillOrderMutex = new Object();
    public static ExecProtos.ExecMessage fillOrder(OrderProtos.OrderMessage _ordMsg)
    {
        synchronized (fillOrderMutex) {
            ExecProtos.ExecMessage eMsg;
            ExecProtos.ExecMessage.Builder bldr = ExecProtos.ExecMessage.newBuilder();
            bldr.setExecId(SimulatorHelper.getNextExecId());
            bldr.setExecOrderId(_ordMsg.getOrderId());
            Date today = Date.from(Instant.now());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            String sTodayDt = sdf.format(today);
            Calendar cal = Calendar.getInstance();
            cal.setTime(today);
            cal.add(Calendar.DATE, 2);
            if (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY) {
                cal.add(Calendar.DATE, 2);
            } else if (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
                cal.add(Calendar.DATE, 1);
            }
            String sSettleDt = sdf.format(Date.from(cal.toInstant()));
            int iRemainingQty = _ordMsg.getQuantity() - _ordMsg.getQuantityFilled();
            if (iRemainingQty == 0) {
                System.out.println("ERROR: Order is completely filled.  Returning default Execution!");
                return bldr.getDefaultInstanceForType();
            }
            double x = Math.random() * 100 + iRemainingQty;
            int iQty = (int) (x % iRemainingQty);
            if (iQty == 0)
                iQty = 1;
            bldr.setQuantity(iQty);
            bldr.setPrice(((float) (Math.random() % 100) * 100));
            bldr.setTradeDate(Integer.parseInt(sTodayDt));
            bldr.setSettleDate(Integer.parseInt(sSettleDt));
            bldr.setAuditTime(Instant.now().getEpochSecond());
            bldr.setAuditUser("CTrader");
            bldr.setExecStatus("NEW");
            bldr.setExecComment("TheComment::ExecID:" + bldr.getExecId());
            return bldr.build();
        }
    }

    private final static Object orderIdMutex = new Object();
    public static synchronized int getNextOrderId()
    {
        synchronized(orderIdMutex)
        {
            return ++ordId;
        }
    }
    private final static Object execIdMutex = new Object();
    public static synchronized int getNextExecId()
    {
        synchronized(execIdMutex)
        {
            return ++execId;
        }
    }
}
