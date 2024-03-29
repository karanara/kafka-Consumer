package com.example.apachekafka.kafkaConsumer.service;

import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import com.example.apachekafka.kafkaConsumer.entity.LibraryEvent;
import com.example.apachekafka.kafkaConsumer.jpa.LibraryEventsRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LibraryEventsService {

	@Autowired
	ObjectMapper objectMapper;
	@Autowired
	private LibraryEventsRepository libraryEventsRepository;
	
	public void processLibraryEvent(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("libraryEvent : {} ", libraryEvent);

        if(libraryEvent!=null && libraryEvent.getLibraryEventId()==999) {
        	throw new RecoverableDataAccessException("Temporary Network issue");
        }
        switch(libraryEvent.getLibraryEventType()){
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                //validate the libraryevent
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("Invalid Library Event Type");
        }

    }

	private void validate(LibraryEvent libraryEvent) {
		
		if(libraryEvent.getLibraryEventId()==null){
            throw new IllegalArgumentException("Library Event Id is missing");
        }

        Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
        if(!libraryEventOptional.isPresent()){
            throw new IllegalArgumentException("Not a valid library Event");
        }
        log.info("Validation is successful for the library Event : {} ", libraryEventOptional.get());
		
	}

	private void save(LibraryEvent libraryEvent) {
		// TODO Auto-generated method stub
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventsRepository.save(libraryEvent);
		log.info("SuccessFully Perisisted the libraryEvent : {} ",libraryEvent);
	}
	
}
