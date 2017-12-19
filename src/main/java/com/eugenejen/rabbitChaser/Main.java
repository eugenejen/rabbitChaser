package com.eugenejen.rabbitChaser;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.rabbitmq.client.impl.StandardMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.io.File;
import java.net.URI;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private String mode;
    private URI rabbitmqUri;
    private MetricRegistry registry;
    private CachingConnectionFactory factory;
    private ExecutorService threadPool;
    private ConsoleReporter consoleReporter;
    private CsvReporter csvReporter;
    private Runnable test;

    Main(String rabbitmqUrl, String mode, TestParams testParams, String csvReportPath) throws Exception {
        this.mode = mode;
        this.rabbitmqUri = new URI(rabbitmqUrl);
        this.registry = new MetricRegistry();
        this.init(testParams, csvReportPath);
    }

    public Main init(TestParams testParams, String csvReportPath) throws Exception {
        File path = new File(csvReportPath);
        path.mkdirs();
        this.consoleReporter = ConsoleReporter.forRegistry(this.registry)
                                              .convertRatesTo(TimeUnit.SECONDS)
                                              .convertDurationsTo(TimeUnit.MILLISECONDS)
                                              .build();
        this.csvReporter = CsvReporter.forRegistry(this.registry)
                                      .formatFor(Locale.US)
                                      .convertRatesTo(TimeUnit.SECONDS)
                                      .convertDurationsTo(TimeUnit.MILLISECONDS)
                                      .build(path);
        StandardMetricsCollector collector = new StandardMetricsCollector(this.registry);
        this.factory = new CachingConnectionFactory(rabbitmqUri);
        factory.setCacheMode(testParams.cacheMode);
        factory.setChannelCacheSize(testParams.channelSize);
        factory.setConnectionCacheSize(testParams.connectionSize);
        this.threadPool = new ThreadPoolExecutor(testParams.threadPoolSize,
                                                 testParams.threadPoolSize, 0L,
                                                 TimeUnit.MILLISECONDS,
                                                 new ArrayBlockingQueue<>(2000),
                                                 new ThreadPoolExecutor.CallerRunsPolicy());
        this.factory.getRabbitConnectionFactory().setMetricsCollector(collector);
        RabbitTemplate template = new RabbitTemplate(this.factory);

        if ("send".equals(this.mode) || "feed".equals(this.mode)) {
            this.test = new Feed(threadPool, registry, template, this.mode, testParams);
        } else if ("read".equals(this.mode) || "drain".equals(this.mode) ) {
            this.test = new Drain(threadPool, registry, template, this.mode, testParams);
        }
        return this;

    }

    private void runTest() throws Exception {
        this.csvReporter.start(1, TimeUnit.SECONDS);
        this.consoleReporter.start(1, TimeUnit.SECONDS);
        this.test.run();
        if ("feed".equals(this.mode) || "drain".equals(this.mode)) {
            while(!Thread.interrupted()) {
                Thread.sleep(1000);
            }
        }
        this.threadPool.shutdown();
        this.threadPool.awaitTermination(10, TimeUnit.MINUTES);
    }

    public void endTest() {
        this.consoleReporter.stop();
        this.csvReporter.stop();
        this.factory.destroy();
        this.csvReporter.report();
        this.consoleReporter.report();
    }

    public static void main(String args[]) throws Exception {
        try {
            String rabbitmqUrl = System.getProperty("rabbitmqUrl", "amqp://localhost:5672");
            String mode = System.getProperty("mode", "send").toLowerCase();
            String csvReportPath = System.getProperty("reportPath", "/tmp");
            TestParams testParams = new TestParams();
            testParams.cacheMode = System.getProperty("cacheMode", "channel").equals("channel") ? CachingConnectionFactory.CacheMode.CHANNEL:
                                   CachingConnectionFactory.CacheMode.CONNECTION;
            testParams.channelSize = Integer.parseInt(System.getProperty("channelSize", "1"));
            testParams.connectionSize = Integer.parseInt(System.getProperty("connectionSize", "1"));
            testParams.numberOfTests = Integer.parseInt(System.getProperty("numberOfTests", "1"));
            testParams.threadPoolSize = Integer.parseInt(System.getProperty("threadPoolSize", "1"));
            testParams.minMessageSizeInWords = Integer.parseInt(System.getProperty("minMessageSize","20000"));
            testParams.maxMessageSizeInWords = Integer.parseInt(System.getProperty("maxMessageSize", "40000"));
            testParams.queueName = System.getProperty("queueName", "default");
            testParams.compressed = Boolean.parseBoolean(System.getProperty("compressed", "false"));
            Main main = new Main(rabbitmqUrl, mode, testParams, csvReportPath);
            LOGGER.info("{}", main.toString());
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
