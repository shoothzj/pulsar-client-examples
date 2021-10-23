package com.github.shoothzj.pulsar.client.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;

/**
 * @author hezhangjian
 */
@Slf4j
public class DemoPulsarClientInit {

    private static final DemoPulsarClientInit INSTANCE = new DemoPulsarClientInit();

    private PulsarClient pulsarClient;

    public static DemoPulsarClientInit getInstance() {
        return INSTANCE;
    }

    public void init() throws Exception {
        pulsarClient = PulsarClient.builder()
                .serviceUrl(PulsarConstant.SERVICE_HTTP_URL)
                .build();
    }

    public PulsarClient getPulsarClient() {
        return pulsarClient;
    }
}
