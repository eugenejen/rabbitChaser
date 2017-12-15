package com.eugenejen.rabbitChaser;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import de.svenjacobs.loremipsum.LoremIpsum;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.Random;
import java.util.concurrent.ExecutorService;

public class Send implements Runnable {

    private ExecutorService executorService;
    private Timer timer;
    private RabbitTemplate template;
    private TestParams testParams;
    private LoremIpsum messageGenerator = new LoremIpsum();
    private Random random = new Random();

    Send(ExecutorService executorService, RabbitTemplate template, MetricRegistry metricRegistry, TestParams testParams) {
        this.executorService = executorService;
        this.template = template;
        this.testParams = testParams;
        this.timer = metricRegistry.timer("send");
    }

    private String generateMessage() {
        int messageLength = random.nextInt(
            testParams.maxMessageSizeInWords - testParams.minMessageSizeInWords
                                                              ) + testParams.minMessageSizeInWords;
        return messageGenerator.getWords(messageLength);
    }

    private void sendMessage() {
        try (Timer.Context t = timer.time()) {
            String message = generateMessage();
            this.template.convertAndSend("default", message);
        }
    }

    @Override
    public void run() {
        for (int i = 0; i < testParams.numberOfTests; i++) {
            this.executorService.submit(this::sendMessage);
        }
    }
}
