package com.css.tradesimulator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class ExecutionSimulatorMain {

    public static void main(String[] args)
    {
        int iArgsCount = 0;
        for (String sArg : args)
        {
            System.out.println("ARG"+iArgsCount++ + "::"+sArg);
        }

        Arrays.stream(args).forEach( (k) -> System.out.println("LAMBDA ARGS::"+k));
        while (true) {
            System.out.println("Enter number of generators to launch or 'exit' or 0 to stop: ");
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                String sInput = reader.readLine();
                if (sInput.equals("0") || sInput.equalsIgnoreCase("exit")) {
                    System.out.println("Received exit code.  EXITING NOW ...");
                    break;
                }
                int nbrOfGenerators = Integer.parseInt(sInput);
                for (int i=1; i <= nbrOfGenerators; i++)
                {
                    String sFileName = args[0] + "\\" + args[1] + i + ".txt";
                    SimulationFileWriter sfw = new SimulationFileWriter(sFileName,i);
                    if (sfw.initialize() == false)
                        break;
                    SimulationEngine objSE = new SimulationEngine(i, 2L, sfw);
                    new Thread(objSE).start();
                }

                System.out.println("Starting "+nbrOfGenerators+" execution simulation generators...");
            } catch (IOException iox) {
                System.out.println("Error reading input from user at start up.");
                System.out.println(iox.toString());
                iox.printStackTrace();
            } catch (Exception ex) {
                System.out.println("General exception received handling input from prompt.");
                System.out.println(ex.toString());
                ex.printStackTrace();
            }
        }
    }
}
