package com.example.apachekafka.kafkaConsumer.config;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import com.example.apachekafka.kafkaConsumer.service.FailureService;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class LibraryEventConsumerConfig {
	
	public static final String RETRY = "RETRY";
    public static final String DEAD = "DEAD";
    public static final String SUCCESS = "SUCCESS";

	
	@Autowired
	KafkaTemplate kafkaTemplate;
	
	@Autowired
	FailureService failureService;
	
	@Value("${topics.retry:library-events.RETRY}")
	private String retryTopic;
	
	@Value("${topics.dlt:library-events.DLT}")
	private String deadLetterTopic;
	
	public DeadLetterPublishingRecoverer publisherRecoverer() {
		 DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate
	                , (r, e) -> {
	            log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
	            if (e.getCause() instanceof RecoverableDataAccessException) {
	                return new TopicPartition(retryTopic, r.partition());
	            } else {
	                return new TopicPartition(deadLetterTopic, r.partition());
	            }
	        }
	        );

	        return recoverer;

	}

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
	
	ConsumerRecordRecoverer consumerRecordRecoverer = (record, exception) -> {
        log.error("Exception is : {} Failed Record : {} ", exception, record);
        if (exception.getCause() instanceof RecoverableDataAccessException) {
            log.info("Inside the recoverable logic");
            //Add any Recovery Code here.
            failureService.saveFailedRecord((ConsumerRecord<Integer, String>) record, exception, RETRY);

        } else {
            log.info("Inside the non recoverable logic and skipping the record : {}", record);

        };
    };


	public DefaultErrorHandler errorHandler() {
		// TODO Auto-generated method stub
		var exceptionsToIgnoreList= List.of(IllegalArgumentException.class);
		var exceptionsToRetryList=List.of(RecoverableDataAccessException.class);
		var fixedBackOff = new FixedBackOff(1000L,2);
		
		var expBackOff = new ExponentialBackOffWithMaxRetries(2);
		expBackOff.setInitialInterval(1000L);
		expBackOff.setMultiplier(2.0);
		expBackOff.setMaxInterval(2000L);
		//var errorHandler= new DefaultErrorHandler(expBackOff);
         var errorHandler = new DefaultErrorHandler(
        		 
        		 //publisherRecoverer(),
        		 consumerRecordRecoverer,
        		 
        		 fixedBackOff);
		//var errorHandler= new DefaultErrorHandler(fixedBackOff);
	    exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
		//exceptionsToRetryList.forEach(errorHandler::addRetryableExceptions);
	    errorHandler.setRetryListeners(
	    		((record,ex,deliveryAttempt)->{
	    			log.info("Failed Record in Retry Listener,Exception :{},deliveryAttempt: {}",ex.getMessage(),deliveryAttempt);
	    		})
	    		
	    		);
	    return errorHandler;
	}
}
