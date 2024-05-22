package com.example.mqtt;

import com.hivemq.client.mqtt.datatypes.MqttQos;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Test {
    public static void main(String[] args) throws InterruptedException {

        var publisher = new MqttPublisher("broker.hivemq.com");
        publisher.connect();
        Runnable publisherTask = () -> {
            var msg = "test...";
            publisher.publish("test/topic", msg, MqttQos.AT_LEAST_ONCE);
            System.out.println("Publish: " + msg);
        };

        var subscriber = new MqttSubscriber("broker.hivemq.com");
        subscriber.connect();
        subscriber.subscribe("test/topic", MqttQos.AT_LEAST_ONCE);
        Runnable consumerTask = () -> {
            Optional<String> message = null;
            try {
                message = subscriber.receiveMessage(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (message.isPresent()) {
                System.out.println("Receive: " + message.get());
            }
        };

        ExecutorService executorService = Executors.newWorkStealingPool();
        while (true) {
            executorService.submit(publisherTask);
            executorService.submit(consumerTask);
            TimeUnit.SECONDS.sleep(2);
            System.out.println();
        }
    }
}
