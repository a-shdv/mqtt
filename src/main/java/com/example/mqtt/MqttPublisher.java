package com.example.mqtt;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class MqttPublisher {

    private final Mqtt5BlockingClient client;

    public MqttPublisher(String serverHost) {
        this.client = MqttClient.builder()
                .serverHost(serverHost)
                .identifier(UUID.randomUUID().toString())
                .useMqttVersion5()
                .buildBlocking();
    }

    public void connect() {
        client.connect();
    }

    public void publish(String topic, String message, MqttQos qos) {
        client.publishWith()
                .topic(topic)
                .qos(qos)
                .payload(message.getBytes())
                .send();
    }

    public void disconnect() {
        client.disconnect();
    }

    public static void main(String[] args) throws InterruptedException {
        MqttPublisher publisher = new MqttPublisher("broker.hivemq.com");
        publisher.connect();
        while (true) {
            var msg = "test...";
            publisher.publish("test/topic", msg, MqttQos.AT_LEAST_ONCE);
            System.out.println("Pub: " + msg);
            TimeUnit.SECONDS.sleep(2);
        }
//        publisher.disconnect();
    }
}