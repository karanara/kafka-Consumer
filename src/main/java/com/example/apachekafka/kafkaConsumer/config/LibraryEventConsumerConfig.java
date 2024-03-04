package com.example.apachekafka.kafkaConsumer.config;

import java.util.List;

import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
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
		var exceptionsToIgnoreList= List.of(IllegalArgumentException.class);
		var exceptionsToRetryList=List.of(RecoverableDataAccessException.class);
		var fixedBackOff = new FixedBackOff(1000L,2);
		var errorHandler= new DefaultErrorHandler(fixedBackOff);
	    //exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
		exceptionsToRetryList.forEach(errorHandler::addRetryableExceptions);
	    errorHandler.setRetryListeners(
	    		((record,ex,deliveryAttempt)->{
	    			log.info("Failed Record in Retry Listener,Exception :{},deliveryAttempt: {}",ex.getMessage(),deliveryAttempt);
	    		})
	    		
	    		);
	    return errorHandler;
	}
}
