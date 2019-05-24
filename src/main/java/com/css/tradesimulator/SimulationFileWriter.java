package com.css.tradesimulator;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

public class SimulationFileWriter implements MsgFileWriterInterface {

    private String sFile;
    private int fileID;
    private FileWriter fw;
    private PrintWriter pw;

    public SimulationFileWriter(String _file, int _id)
    {
        sFile = _file;
        fileID = _id;
    }

    public boolean initialize()
    {
        String myFile = "";
        try {
            myFile = sFile;
            fw = new FileWriter(myFile);
            pw = new PrintWriter(fw);
        }
        catch (IOException iox)
        {
            System.out.println("ERROR: Unable to create file "+sFile);
            iox.printStackTrace();
            return false;
        }
        return true;
    }
    @Override
    public void writeMessage(String sMsg) {
        writeMessageAsBytes(sMsg.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void writeMessageAsBytes(byte[] bArr) {
        try {
            if (pw != null) {
                pw.println(new String(bArr));
            }
            else
                System.out.println("fos is NULL!");
        } catch (Exception iox)
        {
            System.out.println("ERROR:: Writing message to file as Byte array.");
            iox.printStackTrace();
        }
    }

    public void closeWriter()
    {
        try
        {
            if (pw != null)
            {
                pw.flush();
                pw.close();
            }
        }
        catch (Exception iox)
        {

        }

    }
}
