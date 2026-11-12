package com.tuning.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.stream.IntStream;

@RestController
@RequestMapping("/api/test")
public class LoadTestController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public LoadTestController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/start")
    public String startLoadTest(@RequestParam(defaultValue = "10000") int count) {
        long startTime = System.currentTimeMillis();

        // Generar carga masiva paralela
        IntStream.range(0, count).parallel().forEach(i -> {
            String message = "Sensor-Data-" + i + "-Payload-Voluminoso-Para-Llenar-Bytes";
            // Enviar sin esperar respuesta (Fire and Forget para máxima velocidad de prueba)
            kafkaTemplate.send("sensor-topic", "sensor-" + (i % 5), message);
        });

        long endTime = System.currentTimeMillis();
        long duration = Math.max(endTime - startTime, 1); // Evita dividir por 0 con cargas chicas

        return String.format("🚀 Carga completada: %d mensajes enviados en %d ms. Throughput: %.2f msg/sec",
                count, duration, (count / (duration / 1000.0)));
    }
}
