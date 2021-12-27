package com.github.shoothzj.pulsar.client.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.pulsar.PulsarStandalone;
import org.apache.pulsar.PulsarStandaloneBuilder;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.assertj.core.util.Files;

import java.io.File;
import java.util.Optional;

@Slf4j
public class TestPulsarServer {

    private final File zkDir;

    private final File bkDir;

    private final int zkPort;

    private final int bkPort;

    private final int webPort;

    private final int tcpPort;

    private final PulsarStandalone pulsarStandalone;

    public TestPulsarServer() {
        try {
            this.zkPort = TestUtil.getFreePort();
            this.bkPort = TestUtil.getFreePort();
            this.zkDir = Files.newTemporaryFolder();
            this.zkDir.deleteOnExit();
            this.bkDir = Files.newTemporaryFolder();
            this.bkDir.deleteOnExit();
            LocalBookkeeperEnsemble bkEnsemble = new LocalBookkeeperEnsemble(
                    1, zkPort, bkPort, zkDir.toString(),
                    bkDir.toString(), false, "127.0.0.1");
            ServerConfiguration bkConf = new ServerConfiguration();
            bkConf.setJournalRemovePagesFromCache(false);
            log.info("begin to start bookkeeper");
            bkEnsemble.startStandalone(bkConf, false);
            this.webPort = TestUtil.getFreePort();
            this.tcpPort = TestUtil.getFreePort();
            this.pulsarStandalone = PulsarStandaloneBuilder
                    .instance()
                    .withZkPort(zkPort)
                    .withNumOfBk(1)
                    .withOnlyBroker(true)
                    .build();
            ServiceConfiguration standaloneConfig = this.pulsarStandalone.getConfig();
            standaloneConfig.setWebServicePort(Optional.of(webPort));
            standaloneConfig.setBrokerServicePort(Optional.of(tcpPort));
            standaloneConfig.setManagedLedgerDefaultEnsembleSize(1);
            standaloneConfig.setManagedLedgerDefaultWriteQuorum(1);
            standaloneConfig.setManagedLedgerDefaultAckQuorum(1);
            this.pulsarStandalone.setConfig(standaloneConfig);
        } catch (Throwable e) {
            log.error("exception is ", e);
            throw new IllegalStateException("start pulsar standalone failed");
        }
    }

    public void start() throws Exception {
        this.pulsarStandalone.start();
    }

    public int getWebPort() {
        return webPort;
    }

    public int getTcpPort() {
        return tcpPort;
    }

    public void close() throws Exception {
        this.pulsarStandalone.close();
    }
}
