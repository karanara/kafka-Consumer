package com.example.apachekafka.kafkaConsumer.jpa;
import com.example.apachekafka.kafkaConsumer.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;
public interface LibraryEventsRepository extends CrudRepository<LibraryEvent,Integer>{

}
