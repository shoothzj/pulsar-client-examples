# pulsar-client-examples
描述了一些Pulsar客户端编码相关的最佳实践，并提供了可商用的样例代码，供大家研发的时候参考，提升大家接入Pulsar的效率。在生产环境上，Pulsar的地址信息往往都通过配置中心或者是k8s域名发现的方式获得，这块不是这篇文章描述的重点，以`PulsarConstant.SERVICE_HTTP_URL`代替。本文中的例子均已上传到[github](https://github.com/Shoothzj/pulsar-client-examples)

## Client初始化和配置

### 初始化Client--demo级别

```java
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
```

demo级别的Pulsar client初始化的时候没有配置任何自定义参数，并且初始化的时候没有考虑异常，`init`的时候会直接抛出异常。

### 初始化Client--可上线级别

```java
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarClientInitRetry {

    private static final PulsarClientInitRetry INSTANCE = new PulsarClientInitRetry();

    private volatile PulsarClient pulsarClient;

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("pulsar-cli-init"));

    public static PulsarClientInitRetry getInstance() {
        return INSTANCE;
    }

    public void init() {
        executorService.scheduleWithFixedDelay(this::initWithRetry, 0, 10, TimeUnit.SECONDS);
    }

    private void initWithRetry() {
        try {
            pulsarClient = PulsarClient.builder()
                    .serviceUrl(PulsarConstant.SERVICE_HTTP_URL)
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
```



在实际的环境中，我们往往要做到`pulsar client`初始化失败后不影响微服务的启动，即待微服务启动后，再一直重试创建`pulsar client`。<br/>
上面的代码示例通过`volatile`加不断循环重建实现了这一目标，并且在客户端成功创建后，销毁了定时器线程。

### 初始化Client--商用级别

```java
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
```

商用级别的`Pulsar Client`新增了5个配置参数：

- **ioThreads** netty的ioThreads负责网络IO操作，如果业务流量较大，可以调高`ioThreads`个数；
- **listenersThreads** 负责调用以`listener`模式启动的消费者的回调函数，建议配置大于该client负责的`partition`数目；
- **memoryLimit** 当前用于限制`pulsar`生产者可用的最大内存，可以很好地防止网络中断、pulsar故障等场景下，消息积压在`producer`侧，导致java程序OOM；
- **operationTimeout** 一些元数据操作的超时时间，Pulsar默认为30s，有些保守，可以根据自己的网络情况、处理性能来适当调低；
- **connectionTimeout** 连接Pulsar的超时时间，配置原则同上。

### 客户端进阶参数（内存分配相关）

我们还可以通过传递java的property来控制Pulsar客户端内存分配的参数，这里列举几个重要参数

- **pulsar.allocator.pooled** 为true则使用堆外内存池，false则使用堆内存分配，不走内存池。默认使用高效的堆外内存池
- **pulsar.allocator.exit_on_oom** 如果内存溢出，是否关闭**jvm**，默认为false
- **pulsar.allocator.out_of_memory_policy** 在https://github.com/apache/pulsar/pull/12200 引入，目前还没有正式release版本，用于配置当堆外内存不够使用时的行为，可选项为`FallbackToHeap`和`ThrowException`，默认为`FallbackToHeap`，如果你不希望消息序列化的内存影响到堆内存分配，则可以配置成`ThrowException`

## 生产者

### 初始化producer重要参数

#### maxPendingMessages
生产者消息发送队列，根据实际topic的量级合理配置，避免在网络中断、Pulsar故障场景下的OOM。建议和client侧的配置`memoryLimit`之间挑一个进行配置。

### messageRoutingMode

消息路由模式。默认为`RoundRobinPartition`。根据业务需求选择，如果需要保序，通常的做法是在向`Pulsar`发送消息时传递`key`值，这时就会根据`key`来选择要发送到的`partition`。如果有更复杂的保序场景，也可以自定义分发partition的策略。


#### autoUpdatePartition

自动更新partition信息。如`topic`中`partition`信息不变则不需要配置，降低集群的消耗。

#### batch相关参数

因为批量发送模式底层由定时任务实现，如果该topic上消息数较小，则不建议开启`batch`。尤其是大量的低时间间隔的定时任务会导致netty线程CPU飙高。

- **enableBatching** 是否启用批量发送
- **batchingMaxMessages** 批量发送最大消息条数
- **batchingMaxPublishDelay** 批量发送定时任务间隔

### 静态producer初始化

静态producer，指不会随着业务的变化进行producer的启动或关闭。那么就在微服务启动完成、client初始化完成之后，初始化producer，样例如下：

#### 一个生产者一个线程，适用于生产者数目较少的场景

```java
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
```

#### 多个生产者一个线程，适用于生产者数目较多的场景
```java
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarStaticProducersInit {

    private final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("pulsar-producers-init").build();

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, threadFactory);

    private final Map<String, Producer<byte[]>> producerMap = new ConcurrentHashMap<>();

    private int initIndex = 0;

    private final List<String> topics;

    public PulsarStaticProducersInit(List<String> topics) {
        this.topics = topics;
    }

    public void init() {
        executorService.scheduleWithFixedDelay(this::initWithRetry, 0, 10, TimeUnit.SECONDS);
    }

    private void initWithRetry() {
        if (initIndex == topics.size()) {
            executorService.shutdown();
            return;
        }
        for (; initIndex < topics.size(); initIndex++) {
            try {
                final PulsarClientInit instance = PulsarClientInit.getInstance();
                final Producer<byte[]> producer = instance.getPulsarClient().newProducer().topic(topics.get(initIndex)).create();
                producerMap.put(topics.get(initIndex), producer);
            } catch (Exception e) {
                log.error("init pulsar producer error, exception is ", e);
                break;
            }
        }
    }

    public Producer<byte[]> getProducers(String topic) {
        return producerMap.get(topic);
    }

}
```

### 动态生成销毁的producer示例

还有一些业务，我们的producer可能会根据业务来进行动态的启动或销毁，如接收道路上车辆的数据，并发送给指定的topic。我们不会让内存里面驻留所有的producer，这会导致占用大量的内存，我们可以采用类似于LRU Cache的方式来管理producer的生命周期。

```java
/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarDynamicProducerFactory {

    /**
     * topic -- producer
     */
    private AsyncLoadingCache<String, Producer<byte[]>> producerCache;

    public PulsarDynamicProducerFactory() {
        this.producerCache = Caffeine.newBuilder()
                .expireAfterAccess(600, TimeUnit.SECONDS)
                .maximumSize(3000)
                .removalListener((RemovalListener<String, Producer<byte[]>>) (topic, value, cause) -> {
                    log.info("topic {} cache removed, because of {}", topic, cause);
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
            ProducerBuilder<byte[]> builder = DemoPulsarClientInit.getInstance().getPulsarClient().newProducer().enableBatching(true);
            final Producer<byte[]> producer = builder.topic(topic).create();
            future.complete(producer);
        } catch (Exception e) {
            log.error("create producer exception ", e);
            future.completeExceptionally(e);
        }
        return future;
    }

}
```

这个模式下，可以根据返回的`CompletableFuture<Producer<byte[]>>`来优雅地进行流式处理。

### 可以接受消息丢失的发送

```java
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
```

以上为正确处理`Client`创建失败和发送失败的回调函数。但是由于在生产环境下，pulsar并不是一直保持可用的，会因为虚拟机故障、pulsar服务升级等导致发送失败。这个时候如果要保证消息发送成功，就需要对消息发送进行重试。

### 可以容忍极端场景下的发送丢失

```java
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
```

这里在发送失败后，做了退避重试，可以容忍`pulsar`服务端故障一段时间。比如退避7次、初次间隔为1s，那么就可以容忍`1+2+4+8+16+32+64=127s`的故障。这已经足够满足大部分生产环境的要求了。<br/>
因为理论上存在超过127s的故障，所以还是要在极端场景下，向上游返回失败。

### 生产者Partition级别严格保序

生产者严格保序的要点：一次只发送一条消息，确认发送成功后再发送下一条消息。实现上可以使用同步异步两种模式：

- 同步模式的要点就是循环发送，直到上一条消息发送成功后，再启动下一条消息发送
- 异步模式的要点是观测上一条消息发送的future，如果失败也一直重试，成功则启动下一条消息发送

值得一提的是，这个模式下，partition间是可以并行的，可以使用`OrderedExecutor`或`per partition per thread`

同步模式举例：

```java
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarProducerSyncStrictlyOrdered {

    Producer<byte[]> producer;

    public void sendMsg(byte[] msg) {
        while (true) {
            try {
                final MessageId messageId = producer.send(msg);
                log.info("topic {} send success, msg id is {}", producer.getTopic(), messageId);
                break;
            } catch (Exception e) {
                log.error("exception is ", e);
            }
        }
    }

}
```



## 消费者

### 初始化消费者重要参数

#### receiverQueueSize

注意：处理不过来时，消费缓冲队列会积压在内存中，合理配置防止OOM。

#### autoUpdatePartition

自动更新partition信息。如`topic`中`partition`信息不变则不需要配置，降低集群的消耗。

#### subscriptionType

订阅类型，根据业务需求决定。

#### subscriptionInitialPosition

订阅开始的位置，根据业务需求决定放到最前或者最后。

#### messageListener

使用listener模式消费，只需要提供回调函数，不需要主动执行`receive()`拉取。一般没有特殊诉求，建议采用listener模式。

#### ackTimeout

当服务端推送消息，但消费者未及时回复ack，经过ackTimeout后，会重新推送给消费者处理，即`redeliver`机制。<br/>
注意在利用`redeliver`机制的时候，一定要注意仅仅使用重试机制来重试可恢复的错误。举个例子，如果代码里面对消息进行解码，解码失败就不适合利用`redeliver`机制。这会导致客户端一直处于重试之中。

如果拿捏不准，还可以通过下面的`deadLetterPolicy`配置死信队列，防止消息一直重试。

#### negativeAckRedeliveryDelay

当客户端调用`negativeAcknowledge`时，触发`redeliver`机制的时间。`redeliver`机制的注意点同`ackTimeout`。

需要注意的是, `ackTimeout`和`negativeAckRedeliveryDelay`建议不要同时使用，一般建议使用`negativeAck`，用户可以有更灵活的控制权。一旦`ackTimeout`配置的不合理，在消费时间不确定的情况下可能会导致消息不必要的重试。

#### deadLetterPolicy

配置`redeliver`的最大次数和死信topic。


### 初始化消费者原则

消费者只有创建成功才能工作，不像生产者可以向上游返回失败，所以消费者要一直重试创建。示例代码如下：
注意：消费者和topic可以是一对多的关系，消费者可以订阅多个topic。

#### 一个消费者一个线程，适用于消费者数目较少的场景
```java
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarConsumerInit {

    private final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("pulsar-consumer-init").build();

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, threadFactory);

    private final String topic;

    private volatile Consumer<byte[]> consumer;

    public PulsarConsumerInit(String topic) {
        this.topic = topic;
    }

    public void init() {
        executorService.scheduleWithFixedDelay(this::initWithRetry, 0, 10, TimeUnit.SECONDS);
    }

    private void initWithRetry() {
        try {
            final PulsarClientInit instance = PulsarClientInit.getInstance();
            consumer = instance.getPulsarClient().newConsumer().topic(topic).messageListener(new DummyMessageListener<>()).subscribe();
            executorService.shutdown();
        } catch (Exception e) {
            log.error("init pulsar producer error, exception is ", e);
        }
    }

    public Consumer<byte[]> getConsumer() {
        return consumer;
    }
}
```

#### 多个消费者一个线程，适用于消费者数目较多的场景
```java
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarConsumersInit {

    private final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("pulsar-consumers-init").build();

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, threadFactory);

    private final Map<String, Consumer<byte[]>> consumerMap = new ConcurrentHashMap<>();

    private int initIndex = 0;

    private final List<String> topics;

    public PulsarConsumersInit(List<String> topics) {
        this.topics = topics;
    }

    public void init() {
        executorService.scheduleWithFixedDelay(this::initWithRetry, 0, 10, TimeUnit.SECONDS);
    }

    private void initWithRetry() {
        if (initIndex == topics.size()) {
            executorService.shutdown();
            return;
        }
        for (; initIndex < topics.size(); initIndex++) {
            try {
                final PulsarClientInit instance = PulsarClientInit.getInstance();
                final Consumer<byte[]> consumer = instance.getPulsarClient().newConsumer().topic(topics.get(initIndex)).messageListener(new DummyMessageListener<>()).subscribe();
                consumerMap.put(topics.get(initIndex), consumer);
            } catch (Exception e) {
                log.error("init pulsar producer error, exception is ", e);
                break;
            }
        }
    }

    public Consumer<byte[]> getConsumer(String topic) {
        return consumerMap.get(topic);
    }
}
```

### 消费者达到至少一次语义

使用手动回复ack模式，确保处理成功后再ack。如果处理失败可以自己重试或通过`negativeAck`机制进行重试

#### 同步模式举例

这里需要注意，如果处理消息时长差距比较大，同步处理的方式可能会让本来可以很快处理的消息得不到处理的机会。

```java
/**
 * @author hezhangjian
 */
@Slf4j
public class MessageListenerSyncAtLeastOnce<T> implements MessageListener<T> {

    @Override
    public void received(Consumer<T> consumer, Message<T> msg) {
        try {
            final boolean result = syncPayload(msg.getData());
            if (result) {
                consumer.acknowledgeAsync(msg);
            } else {
                consumer.negativeAcknowledge(msg);
            }
        } catch (Exception e) {
            // 业务方法可能会抛出异常
            log.error("exception is ", e);
            consumer.negativeAcknowledge(msg);
        }
    }

    /**
     * 模拟同步执行的业务方法
     * @param msg 消息体内容
     * @return
     */
    private boolean syncPayload(byte[] msg) {
        return System.currentTimeMillis() % 2 == 0;
    }

}
```

#### 异步模式举例

异步的话需要考虑内存的限制，因为异步的方式可以很快地从`broker`消费，不会被业务操作阻塞，这样 **inflight** 的消息可能会非常多。如果是`Shared`或`KeyShared`模式，可以通过`maxUnAckedMessage`进行限制。如果是`Failover`模式，可以通过下面的`消费者繁忙时阻塞拉取消息，不再进行业务处理`通过判断**inflight**消息数来阻塞处理。

```java
/**
 * @author hezhangjian
 */
@Slf4j
public class MessageListenerAsyncAtLeastOnce<T> implements MessageListener<T> {

    @Override
    public void received(Consumer<T> consumer, Message<T> msg) {
        try {
            asyncPayload(msg.getData(), new DemoSendCallback() {
                @Override
                public void callback(Exception e) {
                    if (e == null) {
                        consumer.acknowledgeAsync(msg);
                    } else {
                        log.error("exception is ", e);
                        consumer.negativeAcknowledge(msg);
                    }
                }
            });
        } catch (Exception e) {
            // 业务方法可能会抛出异常
            consumer.negativeAcknowledge(msg);
        }
    }

    /**
     * 模拟异步执行的业务方法
     * @param msg 消息体
     * @param sendCallback 异步函数的callback
     */
    private void asyncPayload(byte[] msg, DemoSendCallback sendCallback) {
        if (System.currentTimeMillis() % 2 == 0) {
            sendCallback.callback(null);
        } else {
            sendCallback.callback(new Exception("exception"));
        }
    }

}
```



### 消费者繁忙时阻塞拉取消息，不再进行业务处理

当消费者处理不过来时，通过阻塞`listener`方法，不再进行业务处理。避免在微服务积累太多消息导致OOM，可以通过RateLimiter或者Semaphore控制处理。

```java
**
 * @author hezhangjian
 */
@Slf4j
public class MessageListenerBlockListener<T> implements MessageListener<T> {

    /**
     * Semaphore保证最多同时处理500条消息
     */
    private final Semaphore semaphore = new Semaphore(500);

    @Override
    public void received(Consumer<T> consumer, Message<T> msg) {
        try {
            semaphore.acquire();
            asyncPayload(msg.getData(), new DemoSendCallback() {
                @Override
                public void callback(Exception e) {
                    semaphore.release();
                    if (e == null) {
                        consumer.acknowledgeAsync(msg);
                    } else {
                        log.error("exception is ", e);
                        consumer.negativeAcknowledge(msg);
                    }
                }
            });
        } catch (Exception e) {
            semaphore.release();
            // 业务方法可能会抛出异常
            consumer.negativeAcknowledge(msg);
        }
    }

    /**
     * 模拟异步执行的业务方法
     * @param msg 消息体
     * @param sendCallback 异步函数的callback
     */
    private void asyncPayload(byte[] msg, DemoSendCallback sendCallback) {
        if (System.currentTimeMillis() % 2 == 0) {
            sendCallback.callback(null);
        } else {
            sendCallback.callback(new Exception("exception"));
        }
    }

}
```

### 消费者严格按partition保序

为了实现`partition`级别消费者的严格保序，需要对单`partition`的消息，一旦处理失败，在这条消息重试成功之前不能处理该`partition`的其他消息。示例如下：

```java
/**
 * @author hezhangjian
 */
@Slf4j
public class MessageListenerSyncAtLeastOnceStrictlyOrdered<T> implements MessageListener<T> {

    @Override
    public void received(Consumer<T> consumer, Message<T> msg) {
        retryUntilSuccess(msg.getData());
        consumer.acknowledgeAsync(msg);
    }

    private void retryUntilSuccess(byte[] msg) {
        while (true) {
            try {
                final boolean result = syncPayload(msg);
                if (result) {
                    break;
                }
            } catch (Exception e) {
                log.error("exception is ", e);
            }
        }
    }

    /**
     * 模拟同步执行的业务方法
     *
     * @param msg 消息体内容
     * @return
     */
    private boolean syncPayload(byte[] msg) {
        return System.currentTimeMillis() % 2 == 0;
    }

}
```

## 致谢

感谢 [鹏辉哥](https://github.com/codelipenghui)和 [罗天](https://github.com/fu-turer)的审稿。

## 作者简介
贺张俭，西安电子科技大学毕业，华为云物联网高级工程师
简书博客地址: https://www.jianshu.com/u/9e21abacd418
