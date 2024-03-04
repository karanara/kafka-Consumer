package com.example.apachekafka.kafkaConsumer.config;

import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class LibraryEventConsumerConfig {

	@Bean
	ConcurrentKafkaListenerContainerFactory<?,?> kafkaListenerConainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,ConsumerFactory<Object,Object> kafkaConsumerFactory
			){
		
		ConcurrentKafkaListenerContainerFactory<Object,Object> factory=new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory);
		factory.setConcurrency(3);
		factory.setCommonErrorHandler(errorHandler());
		return null;
		
	}

	public DefaultErrorHandler errorHandler() {
		// TODO Auto-generated method stub
		var fixedBackOff = new FixedBackOff(1000L,2);
		
		return new DefaultErrorHandler(fixedBackOff);
	}
}
