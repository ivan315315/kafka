package com.example.kafka.demo.config;

import com.example.kafka.demo.utils.ConstantData;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

//https://docs.spring.io/spring-kafka/reference/html/
@Configuration
public class KafkaTopicConfig {
    @Bean
    public NewTopic topicExample() {
        return TopicBuilder.name(ConstantData.testTopic1)
                .partitions(3)
                //.replicas(3)
                .build();
    }

    @Bean
    public NewTopic myTopic2() {
        return TopicBuilder.name(ConstantData.topic2)
                .partitions(2)
                .build();
    }
}
