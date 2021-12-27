package com.github.shoothzj.pulsar.client.examples;

import org.apache.pulsar.client.api.PulsarClient;

public class PulsarClientInitFactory {

    public static PulsarClient acquirePulsarFactory(int webPort) throws Exception {
        return PulsarClient.builder().serviceUrl(String.format(PulsarConstant.SERVICE_HTTP_TEMPLATE, webPort)).build();
    }

}
