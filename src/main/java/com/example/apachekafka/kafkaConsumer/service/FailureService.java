package com.example.apachekafka.kafkaConsumer.service;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import com.example.apachekafka.kafkaConsumer.entity.FailureRecord;
import com.example.apachekafka.kafkaConsumer.jpa.FailureRecordRepository;

@Service
public class FailureService {

    private FailureRecordRepository failureRecordRepository;

    public FailureService(FailureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }

    public void saveFailedRecord(ConsumerRecord<Integer, String> record, Exception exception, String recordStatus){
        var failureRecord = new FailureRecord(null,record.topic(), record.key(),  record.value(), record.partition(),record.offset(),
                exception.getCause().getMessage(),
                recordStatus);

        failureRecordRepository.save(failureRecord);

    }
}