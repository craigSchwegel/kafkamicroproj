/*
 * TradeSimulationMain is the main class for creating the trade simulation micro services.
 * This class has ability to create an engine that publishes String values or byte[] depending
 * if SimulationProducerEngine (String) or ByteSimulationProducerEngine (byte[]) are created.
 * The class runs the services on a separate Thread so that multiple services can be launched
 * from the same main class.
 *
 * @author  Craig Schwegel
 * @version 1.0
 * @since   2019-05-24
 */

package com.css.tradesimulator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import com.css.kafka.ProduerConsumerCreator;


public class TradeSimulationMain {

    public static void main(String[] args) {
        int iArgsCount = 0;
        for (String sArg : args) {
            System.out.println("TradeSimulationMain:ARG" + iArgsCount++ + "::" + sArg);
        }

        Arrays.stream(args).forEach((k) -> System.out.println("TradeSimulationMain:LAMBDA ARGS::" + k));
        //SimulationProducerEngine spe = new SimulationProducerEngine(22, 100, ProduerConsumerCreator.createOrderProducer(), ProduerConsumerCreator.createExecutionProducer());
        ByteSimulationProducerEngine spe = new ByteSimulationProducerEngine(22, 100, ProduerConsumerCreator.createByteOrderProducer(), ProduerConsumerCreator.createByteExecutionProducer());
        Executor executor = Executors.newSingleThreadExecutor();
        try {
            Future futureThread = ((ExecutorService) executor).submit(spe);
            while (true)
            {
                System.out.print("Type exit to quit: ");
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(System.in));
                String sInput = reader.readLine();
                if (sInput.equalsIgnoreCase("exit"))
                {
                    System.out.println("TradeSimulationMain::Exit code received. Shutting down Thread.");
                    futureThread.cancel(true);
                    break;
                }
            }

        } catch (Exception e)
        {
            System.out.println("TradeSimulationMain:Caught ThreadInterruptException...");
            e.printStackTrace();
        }
        System.out.println("TradeSimulationMain::Exiting program...");
    }
}
