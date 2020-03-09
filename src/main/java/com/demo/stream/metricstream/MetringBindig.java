package com.demo.stream.metricstream;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;

public interface MetringBindig {
	
	String INPUT = "input";
	@Input(INPUT)
	KStream<String,String> process();
}
