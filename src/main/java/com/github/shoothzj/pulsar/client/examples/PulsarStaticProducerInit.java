package com.github.shoothzj.pulsar.client.examples;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarStaticProducerInit {

    private final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("pulsar-producer-init").build();

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, threadFactory);

    private final String topic;

    private volatile Producer<byte[]> producer;

    public PulsarStaticProducerInit(String topic) {
        this.topic = topic;
    }

    public void init() {
        executorService.scheduleWithFixedDelay(this::initWithRetry, 0, 10, TimeUnit.SECONDS);
    }

    private void initWithRetry() {
        try {
            final PulsarClientInit instance = PulsarClientInit.getInstance();
            producer = instance.getPulsarClient().newProducer().topic(topic).create();
            executorService.shutdown();
        } catch (Exception e) {
            log.error("init pulsar producer error, exception is ", e);
        }
    }

    public Producer<byte[]> getProducer() {
        return producer;
    }

}
