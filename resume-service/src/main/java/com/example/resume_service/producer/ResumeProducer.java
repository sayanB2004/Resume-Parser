package com.example.resume_service.producer;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ResumeProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void send(Long resumeId, String text) {

        String message = resumeId + "||" + text;
        System.out.println("Sending to Kafka: " + message);

        try {
            kafkaTemplate
                    .send("resume-topic", message)
                    .get();

            System.out.println("Message successfully sent to Kafka ✅"+message);

        } catch (Exception e) {
            System.out.println("❌ Kafka send failed!");
            e.printStackTrace();
        }
    }
}