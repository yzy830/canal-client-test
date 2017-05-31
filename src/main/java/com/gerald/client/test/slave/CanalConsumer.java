package com.gerald.client.test.slave;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.client.impl.ClusterCanalConnector;
import com.alibaba.otter.canal.protocol.Message;

public class CanalConsumer {
    private static final Logger logger = LoggerFactory.getLogger(CanalConsumer.class);
    
    public interface MessageConsumer {
        void consumer(Message message);
    }
    
    private CanalConnector connector;
    
    private MessageConsumer consumer;
    
    private ExecutorService executor;
    
    private String consumerName;
    
    // 标记CanalConsumer是否已经启动
    private boolean isStarted = false;
    
    // 标记消费线程是否在运行中
    private boolean isRunning = false;
    
    private int batchSize;
    
    private String filter;
    
    private List<Long> messageIds = new LinkedList<>();
    
    private int batchTimeout;
    
    private int batchAckSize;
    
    private AtomicBoolean taskTerminated = new AtomicBoolean(true);
    
    public static class CanalConsumerBuilder {
        private InetSocketAddress address;
        
        private String destination;
        
        private String username = "";
        
        private String password = "";
        
        private String filter = ".*\\..*";
        
        private MessageConsumer consumer;
        
        private int batchSize = 1000;
        
        private int batchTimeout = 1000;
        
        private int batchAckSize= 100;

        public InetSocketAddress getAddress() {
            return address;
        }

        /**
         * Canal Server地址
         * 
         * @param address
         *          Canal Server地址
         */
        public CanalConsumerBuilder setAddress(InetSocketAddress address) {
            this.address = address;
            return this;
        }

        public String getDestination() {
            return destination;
        }

        /**
         * instance名称
         * 
         * @param destination
         *          instance名称
         */
        public CanalConsumerBuilder setDestination(String destination) {
            this.destination = destination;
            return this;
        }

        public String getUsername() {
            return username;
        }

        /**
         * 用户名称，默认为空
         * 
         * @param username
         *          用户名称
         */
        public CanalConsumerBuilder setUsername(String username) {
            this.username = username;
            return this;
        }

        public String getPassword() {
            return password;
        }

        /**
         * 用户密码，默认为空
         * 
         * @param password
         *          用户密码
         */
        public CanalConsumerBuilder setPassword(String password) {
            this.password = password;
            return this;
        }

        public String getFilter() {
            return filter;
        }

        /**
         * 过滤规则，默认为".*\\..*"，不过滤
         * 
         * @param filter
         *          过滤规则
         */
        public CanalConsumerBuilder setFilter(String filter) {
            this.filter = filter;
            return this;
        }

        public MessageConsumer getConsumer() {
            return consumer;
        }

        /**
         * 消息消费者
         * 
         * @param consumer
         *          消息消费者
         */
        public CanalConsumerBuilder setConsumer(MessageConsumer consumer) {
            this.consumer = consumer;
            return this;
        }

        public int getBatchSize() {
            return batchSize;
        }

        /**
         * 消息批量获取最大值，默认1000。batchSize = 1000，表示获取的一个消息体中，最多包含1000个Entry
         * 
         * @param batchSize
         *          消息批量获取最大值
         */
        public CanalConsumerBuilder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public int getBatchTimeout() {
            return batchTimeout;
        }

        /**
         * 消息批量获取等待时间，默认1000，单位毫秒。例如batchSize = 1000, batchTimeout = 1000，则在以下两种情况会返回消息
         * <ol>
         *   <li>获得1000个Entry</li>
         *   <li>等待1秒</li>
         * </ol> 
         * <p>
         *   batchTimeout设置为0，将阻塞，知道收到batchSize个Entry
         * </p>
         * 
         * @param batchTimeout
         *          消息批量获取等待时间
         */
        public CanalConsumerBuilder setBatchTimeout(int batchTimeout) {
            this.batchTimeout = batchTimeout;
            return this;
        }

        public int getBatchAckSize() {
            return batchAckSize;
        }

        /**
         * 消息批量ACK大小，默认值100
         * 
         * @param batchAckSize
         *          消息批量ACK大小
         */
        public CanalConsumerBuilder setBatchAckSize(int batchAckSize) {
            this.batchAckSize = batchAckSize;
            return this;
        }
        
        public CanalConsumer build() {
            return new CanalConsumer(address, destination, username, password, filter, consumer, batchSize, batchTimeout, batchAckSize);
        }
    }
    
    private CanalConsumer(InetSocketAddress address, String destination, 
                          String username, String password, 
                          String filter, MessageConsumer consumer, 
                          int batchSize, int batchTimeout, int batchAckSize) {
        if((address == null) || StringUtils.isEmpty(destination) || (consumer ==  null)) {
            throw new IllegalArgumentException("must configure <address>, <destination> and <consumer>");
        }
        
        if((username == null) || (password == null)) {
            throw new IllegalArgumentException("<username>, <password> cannot be null");
        }
        
        if((batchSize <= 0) || (batchTimeout < 0) || (batchAckSize < 0)) {
            throw new IllegalArgumentException("<batchSize> must be greate than zero, <batchTimeout>, <batchAckSize> cannot be negtive");
        }
        
        connector = CanalConnectors.newClusterConnector(address.getAddress().getHostAddress(), destination, username, password);
//        connector = CanalConnectors.newSingleConnector(address, destination, username, password);
        
        if(connector instanceof ClusterCanalConnector) {
            ClusterCanalConnector clusterConnector = (ClusterCanalConnector)connector;
            clusterConnector.setRetryTimes(20);
            clusterConnector.setSoTimeout(10000);
            clusterConnector.setRetryInterval(5000);
        }
        
        this.consumer = consumer;
        this.filter = filter;
        this.batchSize = batchSize;
        this.batchTimeout = batchTimeout;
        this.batchAckSize = batchAckSize;
        
        consumerName = "canal consumer<" + address.getAddress().getHostAddress() + ":" + address.getPort() + "," + destination + ">";
    }
    
    public synchronized void start() {
        if(isStarted) {
            logger.warn(consumerName + " has started");
            return;
        }
        
        if(isRunning) {
            logger.warn("Thread<" + Thread.currentThread().getId() + "> for " + consumerName + " is still running, please wait...");
            return;
        }
        
        executor = Executors.newSingleThreadExecutor((r) -> {
            Thread t = new Thread(r);
            t.setName(consumerName);
            
            return t;
        });
        
        startTask();
        
        executor.execute(() -> {
            isRunning = true;
            
            try {
                connector.connect();
                try {
                    connector.subscribe(filter);
                    try {
                        long messageId = -1L;
                        while(!isTerminateTask()) {
                            Message message = connector.getWithoutAck(batchSize, (long)batchTimeout, TimeUnit.MILLISECONDS);
            
                            messageId = message.getId();
                            if((messageId == -1L) || (message.getEntries().size() == 0)) {
                                continue;
                            }
                            
                            try {
                                consumer.consumer(message);
                            } catch(Exception e) {
                                logger.error("message consumer for " + consumerName + " throw exception", e);
                                long tmp = messageId;
                                // 将messageId标记为-1，避免在ack或者rollback异常时，finally误处理messageId
                                messageId = -1L;
                                
                                // 消息处理失败，将之前的消息确认一次。当前消息不确认，下次再处理
                                ack();
                                connector.rollback(tmp);
                            } finally {
                                if(messageId != -1L) {
                                    messageIds.add(messageId);
                                    messageId = -1L;
                                    
                                    if(messageIds.size() >= batchAckSize) {
                                        // 收到batchAckSize个消息，批量确认一次
                                        ack();
                                    }
                                }
                            }
                            
                        }
                    } finally {
                        try {
                            logger.info(consumerName + " was interrupted, ack all received messages");
                            // 任务被终止，确认已经处理完成的消息
                            ack();
                        } finally {
                            // 终止订阅
                            connector.unsubscribe();
                        }
                    }
                } finally {
                    // 终止连接
                    connector.disconnect();
                }
            } finally {
                // 标记线程终止
                isRunning = false;
            }
        });
        
        isStarted = true;
    }
    
    private void ack() {
        Iterator<Long> iterator = messageIds.iterator();
        
        while(iterator.hasNext()) {
            Long messageId = iterator.next();
            //如果消息确认过程中，出现了异常，则不确认后续的消息
            connector.ack(messageId);
            
            iterator.remove();
        }
    }
    
    public synchronized void stop() throws InterruptedException {
        if(!isStarted) {
            logger.warn(consumerName + " has not started");
            return;
        }
        
        logger.info("stopping " + consumerName + "...");
        terminateTask();
        executor.shutdown();
        
        isStarted = false;

        if(!executor.awaitTermination(60, TimeUnit.SECONDS)) {
            logger.warn(consumerName + " was stoped, but the thread is still running");
        }
    }
    
    private void terminateTask() {
        taskTerminated.compareAndSet(false, true);
    }
    
    private boolean isTerminateTask() {
        return taskTerminated.get();
    }
    
    private void startTask() {
        taskTerminated.compareAndSet(true, false);
    }
    
    public boolean isStarted() {
        return isStarted;
    }
    
    public boolean isRunning() {
        return isRunning;
    }
}
