package com.mozilla.bagheera.serializer;

import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;

import kafka.message.Message;
import kafka.serializer.Encoder;

public class BagheeraEncoder implements Encoder<BagheeraMessage> {

    /* (non-Javadoc)
     * @see kafka.serializer.Encoder#toMessage(java.lang.Object)
     */
    @Override
    public Message toMessage(BagheeraMessage bmsg) {
        return new Message(bmsg.toByteArray());
    }
    
}
