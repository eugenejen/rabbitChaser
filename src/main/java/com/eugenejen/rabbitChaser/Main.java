package com.eugenejen.rabbitChaser;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.rabbitmq.client.impl.StandardMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private String mode;
    private URI rabbitmqUri;
    private MetricRegistry registry;
    private CachingConnectionFactory factory;
    private ExecutorService threadPool;
    private ConsoleReporter reporter;
    private Runnable test;

    Main(String rabbitmqUrl, String mode, TestParams testParams) throws Exception {
        this.mode = mode;
        this.rabbitmqUri = new URI(rabbitmqUrl);
        this.registry = new MetricRegistry();
        this.init(testParams);
    }

    public Main init(TestParams testParams) {
        this.reporter = ConsoleReporter.forRegistry(this.registry)
                                       .convertRatesTo(TimeUnit.SECONDS)
                                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                                       .build();
        StandardMetricsCollector collector = new StandardMetricsCollector(this.registry);
        this.factory = new CachingConnectionFactory(rabbitmqUri);
        factory.setCacheMode(testParams.cacheMode);
        factory.setChannelCacheSize(testParams.channelSize);
        factory.setConnectionCacheSize(testParams.connectionSize);
        this.threadPool = Executors.newFixedThreadPool(testParams.threadPoolSize);
        this.factory.getRabbitConnectionFactory().setMetricsCollector(collector);
        RabbitTemplate template = new RabbitTemplate(this.factory);

        if (this.mode.equals("send")) {
            this.test = new Send(threadPool, template, registry, testParams);
        } else if ("read".equals(this.mode) || "drain".equals(this.mode) ) {
            this.test = new Drain(threadPool, registry, template, testParams);
        }
        return this;

    }

    private void runTest() throws Exception {
        this.reporter.start(1, TimeUnit.SECONDS);
        this.test.run();
        this.threadPool.shutdown();
        this.threadPool.awaitTermination(10, TimeUnit.MINUTES);
    }

    public void endTest() {
        this.reporter.stop();
        this.factory.destroy();
        this.reporter.report();
    }

    public static void main(String args[]) throws Exception {
        try {
            String rabbitmqUrl = System.getProperty("rabbitmqUrl", "amqp://localhost:5672");
            String mode = System.getProperty("mode", "send").toLowerCase();
            String logLevel = System.getProperty("logLevel", "info").toUpperCase();
            System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, logLevel);
            TestParams testParams = new TestParams();
            testParams.channelSize = Integer.parseInt(System.getProperty("channelSize", "1"));
            testParams.connectionSize = Integer.parseInt(System.getProperty("connectionSize", "1"));
            testParams.numberOfTests = Integer.parseInt(System.getProperty("numberOfTests", "1"));
            testParams.threadPoolSize = Integer.parseInt(System.getProperty("threadPoolSize", "1"));
            Main main = new Main(rabbitmqUrl, mode, testParams);
            LOGGER.debug("{}", main.toString());
            LOGGER.info("{}", testParams.toString());
            main.runTest();
            main.endTest();
            System.exit(0);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } finally {

        }
    }

    public String toString() {
        return String.format("[rabbitmqUri=%s, connectionFactory=%s]", this.rabbitmqUri, this.factory);
    }

}
