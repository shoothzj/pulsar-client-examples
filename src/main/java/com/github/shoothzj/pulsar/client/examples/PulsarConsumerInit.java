package com.github.shoothzj.pulsar.client.examples;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarConsumerInit {

    private final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("pulsar-consumer-init").build();

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, threadFactory);

    private final String topic;

    private volatile Consumer<byte[]> consumer;

    public PulsarConsumerInit(String topic) {
        this.topic = topic;
    }

    public void init() {
        executorService.scheduleWithFixedDelay(this::initWithRetry, 0, 10, TimeUnit.SECONDS);
    }

    private void initWithRetry() {
        try {
            final PulsarClientInit instance = PulsarClientInit.getInstance();
            consumer = instance.getPulsarClient().newConsumer().topic(topic).messageListener(new DummyMessageListener<>()).subscribe();
            executorService.shutdown();
        } catch (Exception e) {
            log.error("init pulsar producer error, exception is ", e);
        }
    }

    public Consumer<byte[]> getConsumer() {
        return consumer;
    }
}
