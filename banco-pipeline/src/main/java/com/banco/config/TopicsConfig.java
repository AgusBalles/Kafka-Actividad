package com.banco.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

// Kafka Streams no crea el topic de entrada: si "banco-transacciones" no existe
// al arrancar, el pipeline se detiene con MissingSourceTopicException.
// Declarando los topics como beans, Spring los crea antes de iniciar el pipeline.
@Configuration
public class TopicsConfig {

    @Bean
    public NewTopic topicTransacciones() {
        return TopicBuilder.name("banco-transacciones").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic topicAlertas() {
        return TopicBuilder.name("banco-alertas").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic topicTotalesUsuario() {
        return TopicBuilder.name("banco-totales-usuario").partitions(3).replicas(1).build();
    }
}
