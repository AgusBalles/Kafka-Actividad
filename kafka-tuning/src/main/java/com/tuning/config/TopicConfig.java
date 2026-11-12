package com.tuning.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

// Crea "sensor-topic" con 3 particiones al arrancar (equivale al Paso 2 de la guía).
// Si el topic ya existe (por ejemplo, creado a mano con kafka-topics), no hace nada.
@Configuration
public class TopicConfig {

    @Bean
    public NewTopic sensorTopic() {
        return TopicBuilder.name("sensor-topic").partitions(3).replicas(1).build();
    }
}
