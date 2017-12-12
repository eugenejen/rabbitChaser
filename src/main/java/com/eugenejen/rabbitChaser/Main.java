package com.eugenejen.rabbitChaser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.codahale.metrics.MetricRegistry;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import com.rabbitmq.client.impl.StandardMetricsCollector;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private Logger logger;
    private URI rabbitmqUri;
    private StandardMetricsCollector metrics;
    private CachingConnectionFactory factory;
    private RabbitTemplate template;
    private ExecutorService threadPool;

    Main(Logger logger, String rabbitmqUrl) throws Exception {
        this.logger = logger;
        this.rabbitmqUri = new URI(rabbitmqUrl);
    }

    public Main info(String fmt, Object ... args ) {
        this.logger.info(fmt, args);
        return this;
    }

    public Main init(TestParams testParams) {
        MetricRegistry metricRegistry = new MetricRegistry();
        this.metrics = new StandardMetricsCollector(metricRegistry);
        this.factory = new CachingConnectionFactory(rabbitmqUri);
        factory.setCacheMode(testParams.cacheMode);
        factory.setChannelCacheSize(testParams.channelSize);
        factory.setConnectionCacheSize(testParams.connectionSize);
        this.threadPool = Executors.newFixedThreadPool(testParams.numberOfThreads);
        this.factory.getRabbitConnectionFactory().setMetricsCollector(metrics);
        template = new RabbitTemplate(this.factory);
        return this;
    }

    public Main startTest(TestParams testParams) throws Exception {
        for(int i = 0; i < testParams.numberOfTests; i++) {
            this.threadPool.submit(
                () -> {
                    this.template.convertAndSend("default", "Hello World");
                    this.info("send message {}", "Hello World");
                }
            );
        }
        this.threadPool.shutdown();
        this.threadPool.awaitTermination(10, TimeUnit.MINUTES);
        return this;
    }

    public Main endTest(TestParams testParams) throws Exception {
        this.factory.destroy();
        return this;
    }

    public Main reportMetrics() {

        return this;
    }

    public static void main(String args[]) throws Exception {
        try {
            Logger logger = LoggerFactory.getLogger(Main.class);
            String rabbitmqUrl = System.getProperty("rabbitmqUrl", "amqp://localhost:5672");

            TestParams testParams = new TestParams();
            testParams.channelSize = Integer.parseInt(System.getProperty("channelSize", "1"));
            testParams.connectionSize = Integer.parseInt(System.getProperty("connectionsSize", "1"));
            testParams.numberOfTests = Integer.parseInt(System.getProperty("numberOfTests", "1"));

            Main main = new Main(logger, rabbitmqUrl);
            main.init(testParams);

            main.info("{}", main.rabbitmqUri.toString());
            main.info("{}", main.factory.toString());
            main.startTest(testParams);
            main.reportMetrics();
            main.endTest(testParams);
            System.exit(0);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } finally {

        }
    }

}
