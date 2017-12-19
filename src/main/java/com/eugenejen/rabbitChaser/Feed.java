package com.eugenejen.rabbitChaser;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import de.svenjacobs.loremipsum.LoremIpsum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.io.ByteArrayOutputStream;
import java.util.Random;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

public class Feed implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Feed.class);
    private ExecutorService executorService;
    private Timer timer;
    private Meter originalMeter;
    private Meter sentMeter;
    private RabbitTemplate template;
    private TestParams testParams;
    private LoremIpsum messageGenerator = new LoremIpsum();
    private Random random = new Random();
    private AtomicInteger count = new AtomicInteger(0);
    private String mode;
    private Counter threadCounter;
    private MessageProperties messageProperties;

    Feed(ExecutorService executorService, MetricRegistry metricRegistry, RabbitTemplate template, String mode, TestParams testParams) {
        this.executorService = executorService;
        this.template = template;
        this.testParams = testParams;
        this.timer = metricRegistry.timer("send");
        this.originalMeter = metricRegistry.meter("bytes.original.size");
        this.sentMeter = metricRegistry.meter("bytes.send.size");
        this.threadCounter = metricRegistry.counter("number.of.threads");
        this.mode = mode;
        this.messageProperties = new MessageProperties();
        this.messageProperties.setContentType(MessageProperties.CONTENT_TYPE_BYTES);
        if (testParams.compressed) {
            this.messageProperties.setHeader("content-encoding", "gzip");
        }
    }

    private byte[] compress(byte[] data) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length);
        GZIPOutputStream gzip = new GZIPOutputStream(bos);
        gzip.write(data);
        gzip.close();
        byte[] compressed = bos.toByteArray();
        bos.close();
        return compressed;
    }

    private String generateMessage() {
        int messageLength = random.nextInt(
            testParams.maxMessageSizeInWords - testParams.minMessageSizeInWords) +
            testParams.minMessageSizeInWords;
        return messageGenerator.getWords(messageLength);
    }

    private void sendMessage() {
        LOGGER.info("Thread name: {}", Thread.currentThread().getName());
        byte[] messageAsBytes = null;
        String message = null;
        while (("feed".equals(mode) && !Thread.interrupted())
            || ("send".equals(mode) && count.incrementAndGet() <= testParams.numberOfTests)){
            try (Timer.Context t = timer.time()) {
                message = generateMessage();
                messageAsBytes = message.getBytes();
                if (testParams.compressed) {
                    messageAsBytes = this.compress(message.getBytes());
                } else {
                    messageAsBytes = message.getBytes();
                }
                Message messageObject = new Message(messageAsBytes, this.messageProperties);
                this.template.send(testParams.queueName, messageObject);
            } catch (Exception e) {
                messageAsBytes = null;
            } finally {
                this.originalMeter.mark(message == null ? 0 : message.getBytes().length);
                this.sentMeter.mark(messageAsBytes == null ? 0 : messageAsBytes.length);
            }
        }
    }

    @Override
    public void run() {
        for (int i = 0; i < testParams.threadPoolSize; i++) {
            this.executorService.submit(this::sendMessage);
            this.threadCounter.inc();
        }
    }
}
