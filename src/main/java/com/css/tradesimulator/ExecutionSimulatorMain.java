package com.css.tradesimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class ExecutionSimulatorMain {

    final static Logger logger = LoggerFactory.getLogger(ExecutionSimulatorMain.class);
    public static void main(String[] args)
    {
        Arrays.stream(args).forEach( (k) -> logger.info("LAMBDA ARGS::"+k));
        while (true) {
            System.out.print("Enter number of generators to launch or 'exit' or 0 to stop: ");
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                String sInput = reader.readLine();
                logger.debug("Received input from the System: "+sInput);
                if (sInput.equals("0") || sInput.equalsIgnoreCase("exit")) {
                    logger.info("Received exit code.  EXITING NOW ...");
                    break;
                }
                int nbrOfGenerators = Integer.parseInt(sInput);
                logger.debug("Creating "+nbrOfGenerators+" number of Simulation Engines.");
                for (int i=1; i <= nbrOfGenerators; i++)
                {
                    String sFileName = args[0] + "\\" + args[1] + i + ".txt";
                    SimulationFileWriter sfw = new SimulationFileWriter(sFileName,i);
                    if (sfw.initialize() == false)
                        break;
                    SimulationEngine objSE = new SimulationEngine(i, 2L, sfw);
                    new Thread(objSE).start();
                }

                logger.info("Finished starting "+nbrOfGenerators+" execution simulation generators...");
            } catch (IOException iox) {
                logger.error("Error reading input from user at start up.",iox);
            } catch (Exception ex) {
                logger.error("General exception received handling input from prompt.",ex);
            }

        }
    }
}
