package com.rbcode.rbc_kafka_msg;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/produce")
@RequiredArgsConstructor
public class OrderController {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @PostMapping("/message")
    public String sendMessage(@RequestBody Order order) {

        try {
            for(long i = 0; i<1000000; i++) {
                var oder = new Order();
                oder.setId(i);
                oder.setDescription("Order numver : " + i);
                kafkaTemplate.send(SpringKafkaConfig.TOPIC_NAME, oder);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "json message sent succuessfully";
    }
}
