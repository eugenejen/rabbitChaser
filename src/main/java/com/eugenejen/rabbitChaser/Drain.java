package com.eugenejen.rabbitChaser;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.Map;


public class Drain implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Drain.class);
    private ExecutorService executorService;
    private RabbitTemplate template;
    private TestParams testParams;
    private Timer timer;
    private Meter originalMeter;
    private Meter receivedMeter;
    private AtomicInteger count = new AtomicInteger(0);
    private String mode;
    private Counter threadCounter;

    Drain(ExecutorService executorService, MetricRegistry metricRegistry, RabbitTemplate template, String mode, TestParams testParams) {
        this.executorService = executorService;
        this.template = template;
        this.testParams = testParams;
        this.timer = metricRegistry.timer("read");
        this.originalMeter = metricRegistry.meter("bytes.read");
        this.receivedMeter = metricRegistry.meter("bytes.received");
        this.threadCounter = metricRegistry.counter("number.of.threads");
        this.mode = mode;
    }

    public String decompress(byte[] compressed) throws Exception {
        ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
        GZIPInputStream gis = new GZIPInputStream(bis);
        BufferedReader br = new BufferedReader(new InputStreamReader(gis, "UTF-8"));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            sb.append(line);
        }
        br.close();
        gis.close();
        bis.close();
        return sb.toString();
    }

    private void readMessage() {
        LOGGER.info("Thread name: {}", Thread.currentThread().getName());
        String message = null;
        byte[] messageBytes = null;
        do {
            try (Timer.Context t = timer.time()) {
                Message mo = this.template.receive(testParams.queueName);
                messageBytes  = mo.getBody();
                Map<String, Object> headers = mo.getMessageProperties().getHeaders();
                if (headers.containsKey("content-encoding") && headers.get("content-encoding").equals("gzip")) {
                    message = this.decompress(messageBytes);
                } else {
                    message = new String(messageBytes, "UTF-8");
                }
            } catch (Exception e) {
                message = null;
            } finally {
                this.receivedMeter.mark(messageBytes == null ? 0 : messageBytes.length);
                this.originalMeter.mark((message == null) ? 0 : message.length());
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
