/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.latency;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
/**
 * OSPageCacheBusy，我们都知道commitlog 是单线程顺序追加写的，要想实现单线程顺序追加写，就得在追加写之前获取这个锁，
 * 这个OSPageCacheBusy就是按照某个线程持有锁的时间算出来的，当一次写入持有锁时间1s以上，RocketMQ就会认为OSPageCacheBusy，
 * 这个时候就会开启FastFailure机制，将来的请求给快速拒绝掉。
 * @author YuLang
 *
 */
public class BrokerFastFailure {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
        "BrokerFastFailureScheduledThread"));
    private final BrokerController brokerController;

    public BrokerFastFailure(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public static RequestTask castRunnable(final Runnable runnable) {
        try {
            if (runnable instanceof FutureTaskExt) {
                FutureTaskExt object = (FutureTaskExt) runnable;
                return (RequestTask) object.getRunnable();
            }
        } catch (Throwable e) {
            log.error(String.format("castRunnable exception, %s", runnable.getClass().getName()), e);
        }

        return null;
    }

    public void start() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
            	//如果开启快速失败 则清除失效的 请求
                if (brokerController.getBrokerConfig().isBrokerFastFailureEnable()) {
                    cleanExpiredRequest();
                }
            }
        }, 1000, 10, TimeUnit.MILLISECONDS);
    }
    /**
     * 每隔10毫秒去检查队列中的第一个排队节点，如果该节点的排队时间已经超过了 200ms，就会取消该队列中所有已超过 200ms 的请求，
     * 立即向客户端返回失败，这样客户端能尽快进行重试
     * @param blockingQueue
     * @param maxWaitTimeMillsInQueue
     */
    private void cleanExpiredRequest() {
    	//如果写CommitLog 繁忙
        while (this.brokerController.getMessageStore().isOSPageCacheBusy()) {
            try {
                if (!this.brokerController.getSendThreadPoolQueue().isEmpty()) {
                	//取出一个
                    final Runnable runnable = this.brokerController.getSendThreadPoolQueue().poll(0, TimeUnit.SECONDS);
                    if (null == runnable) {
                        break;
                    }

                    final RequestTask rt = castRunnable(runnable);
                    //直接返回 系统繁忙 SYSTEM_BUSY
                    rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format("[PCBUSY_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, size of queue: %d", System.currentTimeMillis() - rt.getCreateTimestamp(), this.brokerController.getSendThreadPoolQueue().size()));
                } else {
                    break;
                }
            } catch (Throwable ignored) {
            }
        }
        //清除 SendThreadPoolQueue 数据 并且 返回系统繁忙
        cleanExpiredRequestInQueue(this.brokerController.getSendThreadPoolQueue(),
            this.brokerController.getBrokerConfig().getWaitTimeMillsInSendQueue());
        //清除 PullThreadPoolQueue 数据 并且 返回系统繁忙
        cleanExpiredRequestInQueue(this.brokerController.getPullThreadPoolQueue(),
            this.brokerController.getBrokerConfig().getWaitTimeMillsInPullQueue());
        //清除 HeartbeatThreadPoolQueue 数据 并且 返回系统繁忙
        cleanExpiredRequestInQueue(this.brokerController.getHeartbeatThreadPoolQueue(),
            this.brokerController.getBrokerConfig().getWaitTimeMillsInHeartbeatQueue());
        //清除 EndTransactionThreadPoolQueue 数据 并且 返回系统繁忙
        cleanExpiredRequestInQueue(this.brokerController.getEndTransactionThreadPoolQueue(), this
            .brokerController.getBrokerConfig().getWaitTimeMillsInTransactionQueue());
    }
  
    void cleanExpiredRequestInQueue(final BlockingQueue<Runnable> blockingQueue, final long maxWaitTimeMillsInQueue) {
        while (true) {
            try {
                if (!blockingQueue.isEmpty()) {
                	//获取(但并不删除)队列第一个 ，队列为空返回null
                    final Runnable runnable = blockingQueue.peek();
                    if (null == runnable) {
                        break;
                    }
                    final RequestTask rt = castRunnable(runnable);
                    if (rt == null || rt.isStopRun()) {
                        break;
                    }

                    final long behind = System.currentTimeMillis() - rt.getCreateTimestamp();
                    if (behind >= maxWaitTimeMillsInQueue) {
                    	//如果大于最大的等待时间 则删除并 返回系统繁忙
                        if (blockingQueue.remove(runnable)) {
                            rt.setStopRun(true);
                            rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format("[TIMEOUT_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, size of queue: %d", behind, blockingQueue.size()));
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } catch (Throwable ignored) {
            }
        }
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }
}
