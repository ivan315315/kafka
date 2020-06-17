package com.example.kafka.demo.config;

import com.example.kafka.demo.dto.PersonDto;
import com.example.kafka.demo.utils.WorkUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.Nullable;
import org.springframework.util.concurrent.ListenableFuture;

import static com.example.kafka.demo.utils.ConstantData.Partition0;
import static com.example.kafka.demo.utils.ConstantData.topic2;

public class KafkaPersonTemplateWrapped {
    private KafkaTemplate<Long, PersonDto> kafkaPersonTemplate;

    public KafkaPersonTemplateWrapped(KafkaTemplate<Long, PersonDto> kafkaPersonTemplate) {
        this.kafkaPersonTemplate = kafkaPersonTemplate;
    }

    public ListenableFuture<SendResult<Long, PersonDto>> send(String topic, Integer partition, Long key, @Nullable PersonDto data) {
        WorkUtils.setTransportInfoSend(topic, partition, data);
        return kafkaPersonTemplate.send(topic, partition, key, data);
    }
}
