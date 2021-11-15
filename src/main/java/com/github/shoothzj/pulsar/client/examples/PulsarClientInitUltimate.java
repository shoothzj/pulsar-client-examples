package com.github.shoothzj.pulsar.client.examples;

import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SizeUnit;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarClientInitUltimate {

    private static final PulsarClientInitUltimate INSTANCE = new PulsarClientInitUltimate();

    private volatile PulsarClient pulsarClient;

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("pulsar-cli-init"));

    public static PulsarClientInitUltimate getInstance() {
        return INSTANCE;
    }

    public void init() {
        executorService.scheduleWithFixedDelay(this::initWithRetry, 0, 10, TimeUnit.SECONDS);
    }

    private void initWithRetry() {
        try {
            pulsarClient = PulsarClient.builder()
                    .serviceUrl(PulsarConstant.SERVICE_HTTP_URL)
                    .ioThreads(4)
                    .listenerThreads(10)
                    .memoryLimit(64, SizeUnit.MEGA_BYTES)
                    .operationTimeout(5, TimeUnit.SECONDS)
                    .connectionTimeout(15, TimeUnit.SECONDS)
                    .build();
            log.info("pulsar client init success");
            this.executorService.shutdown();
        } catch (Exception e) {
            log.error("init pulsar error, exception is ", e);
        }
    }

    public PulsarClient getPulsarClient() {
        return pulsarClient;
    }

}
