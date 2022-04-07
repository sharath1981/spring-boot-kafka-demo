package com.ryana;

import java.util.stream.IntStream;

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

	private static final String TOPIC = "topic_1";
	private static final String GROUP_1 = "group_1";
	private static final String GROUP_2 = "group_2";

	private final KafkaTemplate<String, Object> template;

	@GetMapping("/publish/{message}")
	public String publish(@PathVariable final String message) {
		IntStream.rangeClosed(1, 10)
				.forEach(i -> template.send(TOPIC, message + i));
		return "Message Sent => " + message;
	}

	@KafkaListener(topics = TOPIC, groupId = GROUP_1)
	public void listener1(final String message) {
		log.info("@KafkaListener-1 Received message {}", message);
	}

	@KafkaListener(topics = TOPIC, groupId = GROUP_2)
	public void listener2(final String message) {
		log.info("@KafkaListener-2 Received message {}", message);
	}

	public static void main(String[] args) {
		SpringApplication.run(SpringBootKafkaDemoApplication.class, args);
	}

}
