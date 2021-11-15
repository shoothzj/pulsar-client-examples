package com.github.shoothzj.pulsar.client.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;

import java.util.concurrent.CompletableFuture;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarProducerAsyncStrictlyOrdered {

    Producer<byte[]> producer;

    public void sendMsgAsync(byte[] msg, CompletableFuture<MessageId> future) {
        try {
            producer.sendAsync(msg).whenCompleteAsync((messageId, throwable) -> {
                if (throwable != null) {
                    log.info("send success, id is {}", messageId);
                    future.complete(messageId);
                    return;
                }
                PulsarProducerAsyncStrictlyOrdered.this.sendMsgAsync(msg, future);
            });
        } catch (Exception e) {
            log.error("exception is ", e);
        }
    }

}
