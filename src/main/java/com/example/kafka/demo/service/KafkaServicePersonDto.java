package com.example.kafka.demo.service;

import com.example.kafka.demo.dto.PersonDto;
import com.example.kafka.demo.utils.WorkUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import static com.example.kafka.demo.utils.ConstantData.*;
import static org.slf4j.LoggerFactory.getLogger;

@Service
@Slf4j
public class KafkaServicePersonDto{

    private static final Logger log = getLogger(KafkaServicePersonDto.class);

    private final KafkaTemplate<Long, PersonDto> kafkaPersonTemplate;
    private final ObjectMapper objectMapper;

    public KafkaServicePersonDto(KafkaTemplate<Long, PersonDto> kafkaPersonTemplate, ObjectMapper objectMapper) {
        this.kafkaPersonTemplate = kafkaPersonTemplate;
        this.objectMapper = objectMapper;
    }

    public PersonDto save(PersonDto dto) {
        return null;
    }

    public void send(PersonDto dto) {
        log.info("producer: {}", WorkUtils.writeValueAsString(objectMapper, dto));
        kafkaPersonTemplate.send(topic, dto);
    }

    @KafkaListener(id = Listener1, topics = {topic}, containerFactory = "singleFactory")
    public void consume1(PersonDto dto) {
        consume_work(null, Listener1, dto);
    }

    @KafkaListener(id = Listener2, topics = {topic}, containerFactory = "singleFactory")
    public void consume2(PersonDto dto) {
        consume_work(null, Listener2, dto);
    }

    @KafkaListener(id = Listener3, topics = {topic}, containerFactory = "singleFactory", groupId = Group1)
    public void consume3(PersonDto dto) {
        consume_work(Group1, Listener3, dto);
    }

    @KafkaListener(id = Listener4, topics = {topic}, containerFactory = "singleFactory", groupId = Group1)
    public void consume4(PersonDto dto) {
        consume_work(Group1, Listener4, dto);
    }

    @KafkaListener(id = Listener5, topics = {topic}, containerFactory = "singleFactory", groupId = Group2)
    public void consume5(PersonDto dto) {
        consume_work(Group2, Listener5, dto);
    }

    @KafkaListener(id = Listener6, topics = {topic}, containerFactory = "singleFactory", groupId = Group2)
    public void consume6(PersonDto dto) {
        consume_work(Group2, Listener6, dto);
    }



    private void consume_work(String group, String listener, PersonDto dto) {
        log.info("Group: {}; listener: {}; consumer: {}", group, listener, WorkUtils.writeValueAsString(objectMapper, dto));
        try {
            PersonDto personDto = objectMapper.readValue(WorkUtils.writeValueAsString(objectMapper, dto), PersonDto.class);
            log.info("object: {}", personDto);
            WorkUtils.personDtoMap.put(personDto.getId(), personDto);
        }
        catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
