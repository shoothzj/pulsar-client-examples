package com.github.shoothzj.pulsar.client.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;

/**
 * @author hezhangjian
 */
@Slf4j
public class MessageListenerSyncAtLeastOnceStrictlyOrdered<T> implements MessageListener<T> {

    @Override
    public void received(Consumer<T> consumer, Message<T> msg) {
        retryUntilSuccess(msg.getData());
        consumer.acknowledgeAsync(msg);
    }

    private void retryUntilSuccess(byte[] msg) {
        while (true) {
            try {
                final boolean result = syncPayload(msg);
                if (result) {
                    break;
                }
            } catch (Exception e) {
                log.error("exception is ", e);
            }
        }
    }

    /**
     * 模拟同步执行的业务方法
     *
     * @param msg 消息体内容
     * @return 发送是否成功
     */
    private boolean syncPayload(byte[] msg) {
        return System.currentTimeMillis() % 2 == 0;
    }

}
