package com.ryana;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Log4j2
@RequiredArgsConstructor
@RestController
@SpringBootApplication
public class SpringBootKafkaDemoApplication {

	private final KafkaTemplate<String, Object> template;
	private static final String TOPIC = "my_topic";
	private static final String PARTITION = "my_topic-0";

	@GetMapping("/publish/{message}")
	public String publish(@PathVariable final String message) {
		template.send(TOPIC, message);
		return "Message Sent => "+message;
	}

	@KafkaListener(topics = TOPIC, groupId = PARTITION)
	public void listener(final String message) {
		log.info("@KafkaListener Received message {}", message);
	}

	public static void main(String[] args) {
		SpringApplication.run(SpringBootKafkaDemoApplication.class, args);
	}

}
