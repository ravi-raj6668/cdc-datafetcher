package com.rasa.cdc.datafetcher.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rasa.cdc.datafetcher.entity.EventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, EventDTO> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, EventDTO> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishProcessedMsg(EventDTO smsData) {
        try {
            String jsonString = new ObjectMapper().writeValueAsString(smsData);
            LOG.info("Publishing message to topic: processed.sms_requests, Payload: {}", jsonString);
            kafkaTemplate.send("input.topic", smsData);
        } catch (Exception e) {
            LOG.error("Error while sending message to topic: {} ", e.getMessage());
            throw new RuntimeException("Exception occurred while publishing message", e);
        }
    }

}
