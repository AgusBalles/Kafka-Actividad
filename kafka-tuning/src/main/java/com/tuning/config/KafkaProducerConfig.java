package com.tuning.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // --- TUNING DE ALTO RENDIMIENTO ---

        // 1. Batch Size: Aumentar a 32KB (Default 16KB)
        // Intentará agrupar mensajes hasta llegar a 32KB
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024);

        // 2. Linger MS: Esperar 20ms antes de enviar
        // Esto permite que se acumulen más mensajes en el batch
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 20);

        // 3. Compression: Snappy (Balance entre CPU y Red)
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
