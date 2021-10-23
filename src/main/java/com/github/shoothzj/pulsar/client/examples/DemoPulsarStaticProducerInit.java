package com.github.shoothzj.pulsar.client.examples;

import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public class DemoPulsarStaticProducerInit {

    private volatile Producer<byte[]> xxProducer;

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("pulsar-producer-init"));

    public void init() {
        executorService.scheduleWithFixedDelay(this::initWithRetry, 0, 10, TimeUnit.SECONDS);
    }

    private void initWithRetry() {
        try {
            final DemoPulsarClientInit instance = DemoPulsarClientInit.getInstance();
            xxProducer = instance.getPulsarClient().newProducer().create();
        } catch (Exception e) {
            log.error("init pulsar producer error, exception is ", e);
        }
    }

    public Producer<byte[]> getXxProducer() {
        return xxProducer;
    }

}
