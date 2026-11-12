package com.kafka.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    private static final String TOPIC = "tutorial-particiones";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @RequestMapping(value = "/send", method = { RequestMethod.GET, RequestMethod.POST })
    public String sendMessage(@RequestParam String message,
                              @RequestParam(required = false) String key) {
        kafkaTemplate.send(TOPIC, key, message);
        return "Mensaje enviado: " + message + " | Key: " + key;
    }
}
