package com.eugenejen.rabbitChaser;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class Drain implements Runnable {

    private ExecutorService executorService;
    private RabbitTemplate template;
    private TestParams testParams;
    private Timer timer;
    private Meter meter;
    private AtomicInteger count = new AtomicInteger(0);
    private String mode;
    private Counter threadCounter;

    Drain(ExecutorService executorService, MetricRegistry metricRegistry, RabbitTemplate template, String mode, TestParams testParams) {
        this.executorService = executorService;
        this.template = template;
        this.testParams = testParams;
        this.timer = metricRegistry.timer("read");
        this.meter = metricRegistry.meter("bytes.read");
        this.threadCounter = metricRegistry.counter("number.of.threads");
        this.mode = mode;
    }

    private void readMessage() {
        String message;
        do {
            try (Timer.Context t = timer.time()) {
                message = (String) this.template.receiveAndConvert(testParams.queueName);
                meter.mark((message == null) ? 0 : message.length());
            }
        } while (("drain".equals(mode) && message != null) ||
            ("read".equals(mode) && count.incrementAndGet() <= testParams.numberOfTests));
    }

    @Override
    public void run() {
        for (int i = 0; i < testParams.threadPoolSize; i++) {
            this.executorService.submit(this::readMessage);
            this.threadCounter.inc();
        }
    }
}
