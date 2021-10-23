package com.github.shoothzj.pulsar.client.examples;

import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public class DemoPulsarConsumerInit {

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("pulsar-consumer-init"));

    private final String topic;

    private volatile Consumer<byte[]> consumer;

    public DemoPulsarConsumerInit(String topic) {
        this.topic = topic;
    }

    public void init() {
        executorService.scheduleWithFixedDelay(this::initWithRetry, 0, 10, TimeUnit.SECONDS);
    }

    private void initWithRetry() {
        try {
            final DemoPulsarClientInit instance = DemoPulsarClientInit.getInstance();
            consumer = instance.getPulsarClient().newConsumer().topic(topic).messageListener(new DemoMessageListener<>()).subscribe();
        } catch (Exception e) {
            log.error("init pulsar producer error, exception is ", e);
        }
    }

    public Consumer<byte[]> getConsumer() {
        return consumer;
    }
}
