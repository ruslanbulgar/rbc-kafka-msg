package com.rbcode.rbc_kafka_msg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(groupId = SpringKafkaConfig.GROUP_ID_JSON, topics = SpringKafkaConfig.TOPIC_NAME, containerFactory = SpringKafkaConfig.KAFKA_LISTENER_CONTAINER_FACTORY)
    public void receiveeMessage(Order order) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = mapper.writeValueAsString(order);
        logger.info("Json message received using Kafka listener " + jsonString);
    }
}
