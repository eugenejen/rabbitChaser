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
    private String mode;
    private URI rabbitmqUri;
    private StandardMetricsCollector metrics;
    private CachingConnectionFactory factory;
    private RabbitTemplate template;
    private ExecutorService threadPool;

    Main(Logger logger, String rabbitmqUrl, String mode, TestParams testParams) throws Exception {
        this.logger = logger;
        this.mode = mode;
        this.rabbitmqUri = new URI(rabbitmqUrl);
        this.init(testParams);
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
        this.threadPool = Executors.newFixedThreadPool(testParams.threadPoolSize);
        this.factory.getRabbitConnectionFactory().setMetricsCollector(metrics);
        template = new RabbitTemplate(this.factory);
        return this;
    }

    public Main startTest(TestParams testParams) throws Exception {
        if (this.mode.equals("send")) {
            this.startFiniteTest(testParams);
        } else if (this.mode.equals("read")) {
            this.startFiniteTest(testParams);
        } else if (this.mode.equals("drain")) {
            this.startIndefinteTest(testParams);
        }
        return this;
    }

    private Main startIndefinteTest(TestParams testParams) throws Exception {
        for(int i = 0; i < testParams.threadPoolSize; i++) {
            if (this.mode.equals("drain")) {
                this.drainQueue();
            }
        }
        this.threadPool.shutdown();
        this.threadPool.awaitTermination(10, TimeUnit.MINUTES);
        return this;
    }

    private Main drainQueue() throws Exception {
        this.threadPool.submit(
            () -> {
                String message;
                do {
                    message = (String) this.template.receiveAndConvert("default");
                } while (message != null);
            }
        );
        return this;
    }

    private Main startFiniteTest(TestParams testParams) throws Exception {
        for(int i = 0; i < testParams.numberOfTests; i++) {
            if (this.mode.equals("send")) {
                this.sendToQueue();
            } else if(this.mode.equals("read")) {
                this.readFromQueue();
            }
        }
        this.threadPool.shutdown();
        this.threadPool.awaitTermination(10, TimeUnit.MINUTES);
        return this;
    }


    private Main readFromQueue() throws Exception {
        this.threadPool.submit(
            () -> {
                String message;
                 message = (String) this.template.receiveAndConvert("default");
                 this.info("read message message {}", message);
            }
       );
        return this;
    }

    private Main sendToQueue() throws Exception {
        this.threadPool.submit(
            () -> {
                this.template.convertAndSend("default", "Hello World");
                this.info("send message {}", "Hello World");
            }
        );
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
            String mode = System.getProperty("mode", "send").toLowerCase();
            TestParams testParams = new TestParams();
            testParams.channelSize = Integer.parseInt(System.getProperty("channelSize", "1"));
            testParams.connectionSize = Integer.parseInt(System.getProperty("connectionsSize", "1"));
            testParams.numberOfTests = Integer.parseInt(System.getProperty("numberOfTests", "1"));
            testParams.threadPoolSize = Integer.parseInt(System.getProperty("threadPoolSize", "1"));

            Main main = new Main(logger, rabbitmqUrl, mode, testParams);
            main.info("{}", main.toString());
            main.info("{}", testParams.toString());
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

    public String toString() {
        return String.format("[rabbitmqUri=%s, connectionFactory=%s]", this.rabbitmqUri, this.factory);
    }

}
