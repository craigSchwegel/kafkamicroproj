package com.css.tradesimulator;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimulationFileWriter implements MsgFileWriterInterface {

    final static Logger logger = LoggerFactory.getLogger(SimulationFileWriter.class);
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
            logger.error("Unable to create file "+sFile);
            logger.error("exception creating file ",iox);
            return false;
        }
        logger.info("File created successfully: "+sFile);
        return true;
    }
    @Override
    public void writeMessage(String sMsg) {
        try {
            writeMessageAsBytes(sMsg.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e)
        {
            logger.error("Exception caught converting String message to bytes.",e);
        }
    }

    @Override
    public void writeMessageAsBytes(byte[] bArr) {
        try {
            if (pw != null) {
                pw.println(new String(bArr));
            }
            else
                logger.info("fos is NULL!");
        } catch (Exception iox)
        {
            logger.error("Writing message to file as Byte array.",iox);
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
            logger.error("Exception flushing and closing file.",iox);
        }

    }
}
