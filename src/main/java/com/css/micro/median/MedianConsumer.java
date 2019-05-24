/*
 * MedianConsumer is the starting point for the micro service that calculates and
 * prints the median execution quantity for a stock ticker.  The service processes
 * trades created by the Trade Simulation services.  This service has the added
 * complexity of having to join data from the Order and Execution topics in the Kafka
 * cluster in order to get the ticker from the Order and the quantity from the Execution.
 * Technical: The service spawns 4 threads: 1. Order listener 2. Execution listener 3.
 * Order processor 4. Execution processor. The purpose of the Order and Execution listeners
 * is to move the events received from Kafka on to a work queue.  This frees up the listeners
 * to immediately go back and listen for more events in a typicalProducer/Consumer pattern.
 * The processing is done on the two other threads.  The Order processor thread deserializes
 * the Order object and maps order Id to ticker in a ConcurrentHasMap.  The Execution processor
 * uses the Order Id map to get the ticker.  The median is calculated based on the ticker using
 * the StreamingMedian class.  The output of the process writes the updated median for the ticker
 * the Median kafka topic in format ticker|median.
 *
 * @author  Craig Schwegel
 * @version 1.0
 * @since   2019-05-24
 *
 */
package com.css.micro.median;

import com.css.kafka.IKafkaConstants;
import com.css.kafka.ProduerConsumerCreator;
import com.css.proto.ExecProtos;
import com.css.proto.OrderProtos;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.*;

public class MedianConsumer {

    public static void main(String[] args) {
        System.out.println("ByteExecutionConsumer::main() Starting Consumer...");
        MedianConsumer mc = new MedianConsumer();
        mc.initialize();
        mc.runConsumer();
    }

    public void initialize()
    {
        tickerMedianMutex = new Object();
        orderTickerMap = new ConcurrentHashMap<>();
        tickerMedianMap = new ConcurrentHashMap<>();
        orderQueue = new LinkedBlockingQueue<>();
        execQueue = new LinkedBlockingQueue<>();
    }
    public void runConsumer() {

        System.out.println("Starting runConsumer()...");
        try
        {
            ConsumerProcessor ordMsgProcessor = new ConsumerProcessor(orderQueue, ProduerConsumerCreator.createByteOrderConsumer(), IKafkaConstants.BYTE_ORDER_TOPIC_NAME);
            ExecutorService svcOrdMsg = Executors.newSingleThreadExecutor();
            Future<String> ordMsgProcessorFuture = svcOrdMsg.submit(ordMsgProcessor);

            ConsumerProcessor execMsgProcessor = new ConsumerProcessor(execQueue, ProduerConsumerCreator.createByteExecutionConsumer(), IKafkaConstants.BYTE_EXEC_TOPIC_NAME);
            ExecutorService svcExecMsg = Executors.newSingleThreadExecutor();
            Future<String> execMsgProcessorFuture = svcExecMsg.submit(execMsgProcessor);

            createAndRunOrderThread();

            createAndRunExecutionThread();

            while (true)
            {
                System.out.print("Type exit to quit: ");
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(System.in));
                String sInput = reader.readLine();
                if (sInput.equalsIgnoreCase("exit"))
                {
                    System.out.println("TradeSimulationMain::Exit code received. Shutting down Thread.");
                    ordMsgProcessorFuture.cancel(true);
                    execMsgProcessorFuture.cancel(true);
                    break;
                }
            }

        } catch (Exception e)
        {
            System.out.println("TradeSimulationMain:Caught ThreadInterruptException...");
            e.printStackTrace();
        }
    }

    public void createAndRunOrderThread()
    {
        Runnable r = () -> {
            try {
                while (true) {
                    byte[] ordMessage = orderQueue.take();
                    OrderProtos.OrderMessage o = OrderProtos.OrderMessage.parseFrom(ordMessage);
                    System.out.println("createAndRunOrderThread():: Processing Order ID = "+o.getOrderId()+" :: ticker = "+o.getTicker());
                    orderTickerMap.put(Long.valueOf(o.getOrderId()),o.getTicker());
                    if (!tickerMedianMap.contains(o.getTicker()))
                    {
                        synchronized (tickerMedianMutex)
                        {
                            if (!tickerMedianMap.contains(o.getTicker()))
                            {
                                tickerMedianMap.put(o.getTicker(),new StreamingMedian());
                            }
                        }
                    }
                }
            }
            catch (InterruptedException ie)
            {
                ie.printStackTrace();
            }
            catch (com.google.protobuf.InvalidProtocolBufferException ex)
            {
                System.out.println("ByteOrderConsumer::ERROR parsing Order from bytes[]");
                ex.printStackTrace();
            }
        };
        Thread ordThread = new Thread(r);
        ordThread.start();
    }
    public void createAndRunExecutionThread()
    {
        Producer<String, String> medianProducer = ProduerConsumerCreator.createMedianProducer();
        Runnable r1 = () -> {
            try {
                while (true) {
                    byte[] execMessage = execQueue.take();
                    ExecProtos.ExecMessage e = ExecProtos.ExecMessage.parseFrom(execMessage);
                    String ticker = orderTickerMap.get(Long.valueOf(e.getExecOrderId()));
                    System.out.println("createAndRunExecutionThread():: Processing ticker = "+ticker);
                    if (ticker == null)
                    {
                        int count = 0;
                        while(ticker == null && count < 3) {
                            Thread.sleep(100);
                            ticker = orderTickerMap.get(Long.valueOf(e.getExecOrderId()));
                            count++;
                        }
                        if (ticker == null)
                        {
                            System.out.println("ERROR processing execution. ORDER NOT FOUND after 3 tries!!! orderId="+e.getExecOrderId());
                            continue;
                        }
                    }
                    StreamingMedian sm = tickerMedianMap.get(ticker);
                    if (sm == null)
                    {
                        int count = 0;
                        while(sm == null && count < 3) {
                            Thread.sleep(100);
                            sm = tickerMedianMap.get(ticker);
                            count++;
                        }
                        if (ticker == null)
                        {
                            System.out.println("ERROR processing execution. STREAMING MEDIAN NOT FOUND after 3 tries!!! ticker="+ticker);
                            continue;
                        }
                    }
                    double median = sm.calcMedian(e.getQuantity());
                    String medVal = ticker + "|" + median;
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>(IKafkaConstants.MEDIAN_TOPIC_NAME, ticker, medVal);
                    System.out.println("**** MEDIAN: "+medVal);
                    medianProducer.send(record);
                }
            }
            catch (InterruptedException ie)
            {
                ie.printStackTrace();
            }
            catch (com.google.protobuf.InvalidProtocolBufferException ex)
            {
                System.out.println("ByteOrderConsumer::ERROR parsing Order from bytes[]");
                ex.printStackTrace();
            }
        };
        Thread execThread = new Thread(r1);
        execThread.start();
    }
    //Begin properties
    private Object tickerMedianMutex;
    public ConcurrentHashMap<String, StreamingMedian> getTickerMedianMap() {
        return tickerMedianMap;
    }

    public void setTickerMedianMap(ConcurrentHashMap<String, StreamingMedian> tickerMedianMap) {
        this.tickerMedianMap = tickerMedianMap;
    }

    private ConcurrentHashMap<String, StreamingMedian> tickerMedianMap;

    public ConcurrentHashMap<Long, String> getOrderTickerMap() {
        return orderTickerMap;
    }

    public void setOrderTickerMap(ConcurrentHashMap<Long, String> orderTickerMap) {
        this.orderTickerMap = orderTickerMap;
    }

    private ConcurrentHashMap<Long, String> orderTickerMap;

    public BlockingQueue<byte[]> getOrderQueue() {
        return orderQueue;
    }

    public void setOrderQueue(BlockingQueue<byte[]> orderQueue) {
        this.orderQueue = orderQueue;
    }

    private BlockingQueue<byte[]> orderQueue;

    public BlockingQueue<byte[]> getExecQueue() {
        return execQueue;
    }

    public void setExecQueue(BlockingQueue<byte[]> execQueue) {
        this.execQueue = execQueue;
    }

    private BlockingQueue<byte[]> execQueue;
}
