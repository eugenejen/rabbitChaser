package com.eugenejen.rabbitChaser;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;

public class TestParams {
    public int numberOfTests = 1;
    public CachingConnectionFactory.CacheMode cacheMode = CachingConnectionFactory.CacheMode.CHANNEL;
    public int channelSize = 1;
    public int connectionSize = 1;
    public int threadPoolSize = 1;
    public int minMessageSizeInWords = 20000;
    public int maxMessageSizeInWords = 40000;
    public String queueName = "default";
    public boolean compressed = false;
    public boolean confirmed = false;

    public String toString() {
        return String.format("TestParams [numberOfTests=%d, " +
                             "channelSize=%d, " +
                             "connectionSize=%d, " +
                             "threadPoolSize=%d, " +
                             "minMessageSize=%d, " +
                             "maxMessageSize=%d, " +
                             "queue=%s, " +
                             "compressed=%b, " +
                             "confirmed=%b]",
                             this.numberOfTests, this.channelSize,
                             this.connectionSize, this.threadPoolSize,
                             this.minMessageSizeInWords, this.maxMessageSizeInWords,
                             this.queueName, this.compressed, this.confirmed
                             );
    }
};