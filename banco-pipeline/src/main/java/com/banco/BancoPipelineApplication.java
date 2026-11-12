package com.banco;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams // Necesario para que Spring cree el StreamsBuilder y arranque el pipeline
public class BancoPipelineApplication {

	public static void main(String[] args) {
		SpringApplication.run(BancoPipelineApplication.class, args);
	}

}
