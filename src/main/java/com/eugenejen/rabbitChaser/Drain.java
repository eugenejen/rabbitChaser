package com.eugenejen.rabbitChaser;

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
    private AtomicInteger count = new AtomicInteger(0);

    Drain(ExecutorService executorService, MetricRegistry metricRegistry, RabbitTemplate template, TestParams testParams) {
        this.executorService = executorService;
        this.template = template;
        this.testParams = testParams;
        this.timer = metricRegistry.timer("read");
    }

    private void readMessage() {
        String message;
        do {
            try (Timer.Context t = timer.time()) {
                message = (String) this.template.receiveAndConvert("default");
            }
        } while (message != null || (testParams.numberOfTests > 0 && count.incrementAndGet() >= testParams.numberOfTests));
    }

    @Override
    public void run() {
        for (int i = 0; i < testParams.threadPoolSize; i++) {
            this.executorService.submit(this::readMessage);
        }
    }
}
