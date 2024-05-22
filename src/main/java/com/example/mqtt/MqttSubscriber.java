package com.example.mqtt;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class MqttSubscriber {

    private final Mqtt5BlockingClient client;

    public MqttSubscriber(String serverHost) {
        this.client = MqttClient.builder()
                .serverHost(serverHost)
                .identifier(UUID.randomUUID().toString())
                .useMqttVersion5().buildBlocking();
    }

    public void connect() {
        client.connect();
    }

    public void subscribe(String topic, MqttQos qos) {
        client.subscribeWith()
                .topicFilter(topic)
                .qos(qos)
                .send();
    }

    public Optional<String> receiveMessage(long timeout, TimeUnit unit) throws InterruptedException {
        try (var publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            Optional<Mqtt5Publish> message = publishes.receive(timeout, unit);
            if (message.isPresent()) {
                return Optional.of(new String(message.get().getPayloadAsBytes()));
            } else {
                return Optional.empty();
            }
        }
    }


    public void disconnect() {
        client.disconnect();
    }

    public static void main(String[] args) throws InterruptedException {
        MqttSubscriber subscriber = new MqttSubscriber("broker.hivemq.com");
        subscriber.connect();
        subscriber.subscribe("test/topic", MqttQos.AT_LEAST_ONCE);
        while (true) {
            Optional<String> message = subscriber.receiveMessage(1, TimeUnit.SECONDS);
            if (message.isPresent()) {
                System.out.println("Sub: " + message.get());
            }
        }
//        subscriber.disconnect();
    }
}