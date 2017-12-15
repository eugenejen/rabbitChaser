package com.eugenejen.rabbitChaser;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import de.svenjacobs.loremipsum.LoremIpsum;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class Feed implements Runnable {

    private ExecutorService executorService;
    private Timer timer;
    private RabbitTemplate template;
    private TestParams testParams;
    private LoremIpsum messageGenerator = new LoremIpsum();
    private Random random = new Random();
    private AtomicInteger count = new AtomicInteger(0);
    private String mode;

    Feed(ExecutorService executorService, MetricRegistry metricRegistry, RabbitTemplate template, String mode, TestParams testParams) {
        this.executorService = executorService;
        this.template = template;
        this.testParams = testParams;
        this.timer = metricRegistry.timer("send");
        this.mode = mode;
    }

    private String generateMessage() {
        int messageLength = random.nextInt(
            testParams.maxMessageSizeInWords - testParams.minMessageSizeInWords) +
            testParams.minMessageSizeInWords;
        return messageGenerator.getWords(messageLength);
    }

    private void sendMessage() {
        while (("feed".equals(mode) && !Thread.interrupted())
            || ("send".equals(mode) && count.incrementAndGet() <= testParams.numberOfTests)){
            try (Timer.Context t = timer.time()) {
                String message = generateMessage();
                this.template.convertAndSend("default", message);
            }
        }
    }

    @Override
    public void run() {
        for (int i = 0; i < testParams.threadPoolSize; i++) {
            this.executorService.submit(this::sendMessage);
        }
    }
}
