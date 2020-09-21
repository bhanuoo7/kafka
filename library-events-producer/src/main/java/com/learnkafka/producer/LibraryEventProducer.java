package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.xml.transform.Result;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class LibraryEventProducer {

    @Autowired  //this will invoke spring auto config kafka template
    KafkaTemplate<Integer,String> kafkaTemplate;
    String topic="library-events";

    @Autowired
    ObjectMapper objectMapper; //converts object to string(libraryEvent to string)

    //asynchronous approach
    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
//        controller will invoke LibraryEventProducer class and msg will come from LibraryEventController class
//        let's send the msg to kafka topics
        Integer key = libraryEvent.getLibraryEventId();
        String value=objectMapper.writeValueAsString(libraryEvent);

        ListenableFuture<SendResult<Integer,String>> listenableFuture= kafkaTemplate.sendDefault(key,value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                handleFailure(key,value,throwable);

            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key,value,result);
            }
        });

    }

//    approach 3--this is asynchronous approach
    public void sendLibraryEvent_Approach3(LibraryEvent libraryEvent) throws JsonProcessingException {
//        controller will invoke LibraryEventProducer class and msg will come from LibraryEventController class
//        let's send the msg to kafka topics
        Integer key = libraryEvent.getLibraryEventId();
        String value=objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer,String> producerRecord=buildProducerRecord(key,value,topic);
        ListenableFuture<SendResult<Integer,String>> listenableFuture= kafkaTemplate.send(producerRecord);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

            @Override
            public void onFailure(Throwable throwable) {
            handleFailure(key,value,throwable);

            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key,value,result);
            }
        });

    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {

        List<Header> recordHeaders = List.of(new RecordHeader("event-source","scanner".getBytes()));
        return new ProducerRecord<>(topic,null,key,value,recordHeaders);

    }


    // Syncronous approach
    public SendResult<Integer,String> sendLibraryEventSyncronous(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        Integer key = libraryEvent.getLibraryEventId();
        String value=objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer,String> sendResult=null;

        try {
            sendResult=  kafkaTemplate.sendDefault(key,value).get(5, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException e) {
            log.error("ExecutionException/InterruptedException sending message and the exception is {}",e.getMessage());
            throw e;
//            e.printStackTrace();
        } catch (Exception e) {
            log.error("Exception sending message and the exception is {}",e.getMessage());
            throw e;
        }
        return sendResult;
    }
    private void handleFailure(Integer key, String value, Throwable throwable) {
        log.error("error sending message and the exception is {}",throwable.getMessage());
        try {
            throw throwable;
        } catch (Throwable e) {
            log.error("error on failure: {}",e.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message Sent successfully fro the key:{} and the value is {}, partition is {}",key,value,result.getRecordMetadata().partition());
    }


}
