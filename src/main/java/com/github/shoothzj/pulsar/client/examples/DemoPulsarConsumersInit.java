package com.github.shoothzj.pulsar.client.examples;

import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public class DemoPulsarConsumersInit {

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("pulsar-consumer-init"));

    private CopyOnWriteArrayList<Consumer<byte[]>> consumers;

    private int initIndex;

    private List<String> topics;

    public DemoPulsarConsumersInit(List<String> topics) {
        this.topics = topics;
    }

    public void init() {
        executorService.scheduleWithFixedDelay(this::initWithRetry, 0, 10, TimeUnit.SECONDS);
    }

    private void initWithRetry() {
        if (initIndex == topics.size()) {
            return;
        }
        for (; initIndex < topics.size(); initIndex++) {
            try {
                final DemoPulsarClientInit instance = DemoPulsarClientInit.getInstance();
                final Consumer<byte[]> consumer = instance.getPulsarClient().newConsumer().topic(topics.get(initIndex)).messageListener(new DemoMessageListener<>()).subscribe();
                consumers.add(consumer);
            } catch (Exception e) {
                log.error("init pulsar producer error, exception is ", e);
                break;
            }
        }
    }

    public CopyOnWriteArrayList<Consumer<byte[]>> getConsumers() {
        return consumers;
    }
}
