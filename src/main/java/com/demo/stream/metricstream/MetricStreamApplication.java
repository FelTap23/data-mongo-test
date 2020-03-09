package com.demo.stream.metricstream;

import java.io.IOException;
import java.time.Duration;

import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootApplication
@EnableBinding(MetringBindig.class)
public class MetricStreamApplication {

	private static Logger logger = LoggerFactory.getLogger(MetricStreamApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(MetricStreamApplication.class, args);
	}

	@Autowired
	private MongoTemplate mongoTemplate;

	@StreamListener
	public void process(@Input(MetringBindig.INPUT) KStream<String, String> events) {

		final ObjectMapper mapper = new ObjectMapper();

		events.mapValues((k, v) -> {
			try {
				return mapper.readTree(v);
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}).filter((k, v) -> v != null).foreach((k, v) -> {
			boolean retry = false;
			int retryCount = 0;

			do {
				try {
					mongoTemplate.save(v.toString(), "customer");
					logger.info(String.format("Record %s processed", v.toString()));
					retry = false;
				} catch (Exception possibleMongoException) {
					logger.error(String.format(" Problems to send the message to mongo\nretry number %d %s", ++retryCount, possibleMongoException.toString()));
					retry = true;
				}

				if (retry) {
					try {
						Thread.sleep(Duration.ofSeconds(1).toMillis());
					} catch (InterruptedException interruptedException) {
						logger.error(String.format("%s", interruptedException.toString()));
					}
				}

			} while (retry);
		});

	}

}
