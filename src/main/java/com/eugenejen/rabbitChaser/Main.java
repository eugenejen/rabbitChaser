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

public class Main {

    private Logger logger;
    private URI rabbitmqUri;
    private StandardMetricsCollector metrics;
    private CachingConnectionFactory factory;
    private RabbitTemplate template;

    Main(Logger logger, String rabbitmqUrl) throws Exception {
        this.logger = logger;
        this.rabbitmqUri = new URI(rabbitmqUrl);
    }

    public Main info(String fmt, Object ... args ) {
        this.logger.info(fmt, args);
        return this;
    }

    public Main init() {
        MetricRegistry metricRegistry = new MetricRegistry();
        this.metrics = new StandardMetricsCollector(metricRegistry);
        this.factory = new CachingConnectionFactory(rabbitmqUri);
        this.factory.getRabbitConnectionFactory().setMetricsCollector(metrics);
        template = new RabbitTemplate(this.factory);
        return this;
    }

    public Main startTest() {
        this.template.convertAndSend("default", "Hello World");
        return this;
    }

    public Main reportMetrics() {

        return this;
    }

    public static void main(String args[]) throws Exception {
        try {
            Logger logger = LoggerFactory.getLogger(Main.class);
            String rabbitmqUrl = System.getProperty("rabbitmqUrl", "amqp://guest:guest@localhost:5672/");
            Main main = new Main(logger, rabbitmqUrl);
            main.init();
            main.info("{}", main.rabbitmqUri.toString());
            main.info("{}", main.factory.toString());
            main.startTest();
            main.reportMetrics();
            System.exit(0);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } finally {

        }
    }

}
