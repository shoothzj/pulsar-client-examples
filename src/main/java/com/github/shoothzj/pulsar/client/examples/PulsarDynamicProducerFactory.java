package com.github.shoothzj.pulsar.client.examples;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarDynamicProducerFactory {

    /**
     * topic -- producer
     */
    private final AsyncLoadingCache<String, Producer<byte[]>> producerCache;

    public PulsarDynamicProducerFactory() {
        this.producerCache = Caffeine.newBuilder()
                .expireAfterAccess(600, TimeUnit.SECONDS)
                .maximumSize(3000)
                .removalListener((RemovalListener<String, Producer<byte[]>>) (topic, value, cause) -> {
                    log.info("topic {} cache removed, because of {}", topic, cause);
                    if (value == null) {
                        return;
                    }
                    try {
                        value.close();
                    } catch (Exception e) {
                        log.error("close failed, ", e);
                    }
                })
                .buildAsync(new AsyncCacheLoader<>() {
                    @Override
                    public CompletableFuture<Producer<byte[]>> asyncLoad(String topic, Executor executor) {
                        return acquireFuture(topic);
                    }

                    @Override
                    public CompletableFuture<Producer<byte[]>> asyncReload(String topic, Producer<byte[]> oldValue,
                                                                           Executor executor) {
                        return acquireFuture(topic);
                    }
                });
    }

    private CompletableFuture<Producer<byte[]>> acquireFuture(String topic) {
        CompletableFuture<Producer<byte[]>> future = new CompletableFuture<>();
        try {
            ProducerBuilder<byte[]> builder = PulsarClientInit.getInstance().getPulsarClient().newProducer().enableBatching(true);
            final Producer<byte[]> producer = builder.topic(topic).create();
            future.complete(producer);
        } catch (Exception e) {
            log.error("create producer exception ", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    public void sendMsg(String topic, byte[] msg) {
        final CompletableFuture<Producer<byte[]>> cacheFuture = producerCache.get(topic);
        cacheFuture.whenComplete((producer, e) -> {
            if (e != null) {
                log.error("create pulsar client exception ", e);
                return;
            }
            try {
                producer.sendAsync(msg).whenComplete(((messageId, throwable) -> {
                    if (throwable == null) {
                        log.info("topic {} send success, msg id is {}", topic, messageId);
                        return;
                    }
                    log.error("send producer msg error ", throwable);
                }));
            } catch (Exception ex) {
                log.error("send async failed ", ex);
            }
        });
    }

    private final Timer timer = new HashedWheelTimer();

    public void sendMsgWithRetry(String topic, byte[] msg, int retryTimes, int maxRetryTimes) {
        final CompletableFuture<Producer<byte[]>> cacheFuture = producerCache.get(topic);
        cacheFuture.whenComplete((producer, e) -> {
            if (e != null) {
                log.error("create pulsar client exception ", e);
                return;
            }
            try {
                producer.sendAsync(msg).whenComplete(((messageId, throwable) -> {
                    if (throwable == null) {
                        log.info("topic {} send success, msg id is {}", topic, messageId);
                        return;
                    }
                    if (retryTimes < maxRetryTimes) {
                        log.warn("topic {} send failed, begin to retry {} times exception is ", topic, retryTimes, throwable);
                        timer.newTimeout(timeout -> PulsarDynamicProducerFactory.this.sendMsgWithRetry(topic, msg, retryTimes + 1, maxRetryTimes), 1L << retryTimes, TimeUnit.SECONDS);
                    }
                    log.error("send producer msg error ", throwable);
                }));
            } catch (Exception ex) {
                log.error("send async failed ", ex);
            }
        });
    }

}
