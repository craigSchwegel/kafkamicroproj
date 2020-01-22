package com.css.kafka;

import com.css.proto.ExecProtos;
import com.css.proto.OrderProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface IProtoHelper {

    final static Logger logger = LoggerFactory.getLogger(IProtoHelper.class);
    static OrderProtos.OrderMessage orderMessageConverter(byte[] barr)
    {
        try {
            OrderProtos.OrderMessage ordmsg = OrderProtos.OrderMessage.parseFrom(barr);
            return ordmsg;
        } catch (com.google.protobuf.InvalidProtocolBufferException bufEx) {
            bufEx.printStackTrace();
            logger.error("Exception parsing byte array.",bufEx);
        }
        return null;
    }
    static ExecProtos.ExecMessage execMessageConverter(byte[] earr)
    {
        try {
            ExecProtos.ExecMessage execmsg = ExecProtos.ExecMessage.parseFrom(earr);
            return execmsg;
        } catch (com.google.protobuf.InvalidProtocolBufferException bufEx) {
            bufEx.printStackTrace();
            logger.error("Exception parsing byte array.",bufEx);
        }
        return null;
    }
}
