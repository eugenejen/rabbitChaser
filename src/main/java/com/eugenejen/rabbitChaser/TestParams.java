package com.eugenejen.rabbitChaser;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;

public class TestParams {
    public int numberOfTests = 1;
    public CachingConnectionFactory.CacheMode cacheMode = CachingConnectionFactory.CacheMode.CHANNEL;
    public int channelSize = 1;
    public int connectionSize = 1;
    public int numberOfThreads = 1;
};