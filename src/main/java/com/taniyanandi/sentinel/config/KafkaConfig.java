package com.taniyanandi.sentinel.config;

import com.taniyanandi.sentinel.domain.AnomalyAlert;
import com.taniyanandi.sentinel.domain.TelemetryEvent;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class KafkaConfig {
    @Bean
    ProducerFactory<String, TelemetryEvent> telemetryProducerFactory(KafkaProperties properties) {
        Map<String, Object> props = new HashMap<>(properties.buildProducerProperties(null));
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", JsonSerializer.class);
        props.put("enable.idempotence", true);
        props.put("acks", "all");
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    KafkaTemplate<String, TelemetryEvent> kafkaTemplate(ProducerFactory<String, TelemetryEvent> pf) {
        return new KafkaTemplate<>(pf);
    }

    @Bean
    org.apache.kafka.clients.admin.NewTopic eventsTopic(
            @Value("${sentinel.kafka.events-topic:sentinel.events}") String topic) {
        return TopicBuilder.name(topic).partitions(6).replicas(1).build();
    }

    @Bean
    org.apache.kafka.clients.admin.NewTopic alertsTopic(
            @Value("${sentinel.kafka.alerts-topic:sentinel.alerts}") String topic) {
        return TopicBuilder.name(topic).partitions(6).replicas(1).build();
    }
}
