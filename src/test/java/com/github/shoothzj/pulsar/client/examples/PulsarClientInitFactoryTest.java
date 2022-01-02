package com.github.shoothzj.pulsar.client.examples;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

class PulsarClientInitFactoryTest {

    private static TestPulsarServer testPulsarServer;

    @BeforeAll
    static void initPulsar() throws Exception {
        testPulsarServer = new TestPulsarServer();
        testPulsarServer.start();
    }

    @Test
    @Timeout(value = 15)
    void testPulsarClientInit() throws Exception {
        PulsarClient pulsarClient = PulsarClientInitFactory.acquirePulsarFactory(testPulsarServer.getWebPort());
        TimeUnit.SECONDS.sleep(3);
        Producer<byte[]> producer = pulsarClient.newProducer().topic("test").create();
        Assertions.assertNotNull(producer);
    }

    @AfterAll
    static void closePulsar() throws Exception {
        testPulsarServer.close();
    }

}
