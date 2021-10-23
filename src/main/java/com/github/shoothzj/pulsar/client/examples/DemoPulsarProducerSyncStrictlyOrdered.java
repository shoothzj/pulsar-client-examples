package com.github.shoothzj.pulsar.client.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;

/**
 * @author hezhangjian
 */
@Slf4j
public class DemoPulsarProducerSyncStrictlyOrdered {

    Producer<byte[]> producer;

    public void sendMsg(byte[] msg) {
        while (true) {
            try {
                final MessageId messageId = producer.send(msg);
                log.info("topic {} send success, msg id is {}", producer.getTopic(), messageId);
                break;
            } catch (Exception e) {
                log.error("exception is ", e);
            }
        }
    }

}
