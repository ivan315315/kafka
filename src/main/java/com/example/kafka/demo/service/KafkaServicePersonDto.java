package com.example.kafka.demo.service;

import com.example.kafka.demo.dto.PersonDto;
import com.example.kafka.demo.utils.ConstantData;
import com.example.kafka.demo.utils.WorkUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

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
        kafkaPersonTemplate.send(ConstantData.topic, dto);
    }

    @KafkaListener(id = "Starship", topics = {ConstantData.topic}, containerFactory = "singleFactory")
    public void consume(PersonDto dto) {
        log.info("consumer: {}", WorkUtils.writeValueAsString(objectMapper, dto));
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
