package com.github.shoothzj.pulsar.client.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarClientInit {

    private static final PulsarClientInit INSTANCE = new PulsarClientInit();

    private PulsarClient pulsarClient;

    public static PulsarClientInit getInstance() {
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
