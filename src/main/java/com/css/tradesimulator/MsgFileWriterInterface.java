package com.css.tradesimulator;

public interface MsgFileWriterInterface {
    void writeMessage(String sMsg);
    void writeMessageAsBytes(byte[] bArr);
    void closeWriter();
}
