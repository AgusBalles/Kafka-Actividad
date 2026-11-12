package com.tuning.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class SlowConsumer {

    @KafkaListener(topics = "sensor-topic", groupId = "analytics-group")
    public void listen(String message) {
        // Simulamos un procesamiento pesado (ej. escribir en BD, IA)
        try {
            Thread.sleep(10); // 10ms por mensaje = máx 100 mensajes/segundo
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // No imprimimos para no saturar la consola
    }
}
