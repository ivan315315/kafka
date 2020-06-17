package com.example.kafka.demo.service;

import com.example.kafka.demo.config.KafkaPersonTemplateWrapped;
import com.example.kafka.demo.dto.PersonDto;
import com.example.kafka.demo.utils.WorkUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import static com.example.kafka.demo.utils.ConstantData.*;
import static org.slf4j.LoggerFactory.getLogger;

@Service
@Slf4j
public class KafkaServicePersonDto{

    private static final Logger log = getLogger(KafkaServicePersonDto.class);

    //private static final String Partition0St = Partition0.toString();

    //private final KafkaTemplate<Long, PersonDto> kafkaPersonTemplate;
    private final KafkaPersonTemplateWrapped kafkaPersonTemplateWrapped;
    private final ObjectMapper objectMapper;

    public KafkaServicePersonDto(ObjectMapper objectMapper, KafkaPersonTemplateWrapped kafkaPersonTemplateWrapped) {
        this.kafkaPersonTemplateWrapped = kafkaPersonTemplateWrapped;
        this.objectMapper = objectMapper;
    }

    public PersonDto save (PersonDto dto) {
        return null;
    }

    //https://stackoverflow.com/questions/27036923/how-to-create-a-topic-in-kafka-through-java
    //https://www.programcreek.com/java-api-examples/?class=kafka.admin.AdminUtils&method=createTopic
    //https://analyticshut.com/manage-kafka-topics-in-java/
    //https://www.codota.com/code/java/methods/kafka.admin.AdminUtils/createTopic
    //https://medium.com/@TimvanBaarsen/programmatically-create-kafka-topics-using-spring-kafka-8db5925ed2b1
    public void send(PersonDto dto) {
        //partitions assigned: [mytopic-0]
        //log.info("producer: {}", WorkUtils.writeValueAsString(objectMapper, dto));
        //kafkaPersonTemplate.send(topic, dto);

        PersonDto dto1 = dto;
        //WorkUtils.setTransportInfoSend(topic2, Partition0, dto1);
        log.info("producer: {}", WorkUtils.writeValueAsString(objectMapper, dto1));
        //kafkaPersonTemplate.send(topic2, Partition0, 0l, dto1);
        kafkaPersonTemplateWrapped.send(topic2, Partition0, 0l, dto1);
        //kafkaPersonTemplate.send(topic2, Partition0, 0l, dto1).addCallback(a -> System.out.println(dto1.getKafkaTransportInfo() + ". DTO: " + dto1), System.err::println);
        PersonDto dto2 = dto;
        log.info("producer: {}", WorkUtils.writeValueAsString(objectMapper, dto2));
        kafkaPersonTemplateWrapped.send(topic2, Partition0, 0l, dto2);
        PersonDto dto3 = dto;
        log.info("producer: {}", WorkUtils.writeValueAsString(objectMapper, dto3));
        kafkaPersonTemplateWrapped.send(topic2, Partition0, 0l, dto3);
        PersonDto dto4 = dto;
        log.info("producer: {}", WorkUtils.writeValueAsString(objectMapper, dto4));
        kafkaPersonTemplateWrapped.send(topic2, Partition1, 0l, dto4);
        PersonDto dto5 = dto;
        log.info("producer: {}", WorkUtils.writeValueAsString(objectMapper, dto5));
        kafkaPersonTemplateWrapped.send(topic2, Partition1, 0l, dto5);
        PersonDto dto6 = dto;
        log.info("producer: {}", WorkUtils.writeValueAsString(objectMapper, dto6));
        kafkaPersonTemplateWrapped.send(topic2, Partition1, 0l, dto6);
    }

    //two consumers cannot consume messages from the same partition at the same time. A consumer can consume from multiple partitions at the same time.
   // https://dzone.com/articles/kafka-producer-and-consumer-example

    @KafkaListener(id = Listener1, topics = {topic}, containerFactory = "singleFactory")
    public void consume1(PersonDto dto) {
        consumeWork(null, Listener1, dto, topic, null);
    }

    @KafkaListener(id = Listener2, topics = {topic}, containerFactory = "singleFactory")
    public void consume2(PersonDto dto) {
        consumeWork(null, Listener2, dto, topic, null);
    }

    @KafkaListener(id = Listener3, topics = {topic}, containerFactory = "singleFactory", groupId = Group1)
    public void consume3(PersonDto dto) {
        consumeWork(Group1, Listener3, dto, topic, null);
    }

    @KafkaListener(id = Listener4, topics = {topic}, containerFactory = "singleFactory", groupId = Group1)
    public void consume4(PersonDto dto) {
        consumeWork(Group1, Listener4, dto, topic, null);
    }

    @KafkaListener(id = Listener5, topics = {topic}, containerFactory = "singleFactory", groupId = Group2)
    public void consume5(PersonDto dto) {
        consumeWork(Group2, Listener5, dto, topic, null);
    }

    @KafkaListener(id = Listener6, topics = {topic}, containerFactory = "singleFactory", groupId = Group2)
    public void consume6(PersonDto dto) {
        consumeWork(Group2, Listener6, dto, topic, null);
    }
    //////////////////////////////////////////
    //1message1topic - read all listeners
    //1message1topic - 1group2listeners - read only 1 listener
    ////////
    //1message-1topic-2group[2consume] - Каждая группа прочитает сообщение, при этом только 1 consume из каждой группы прочитает сообщение.
    //1message-1topic-1group[2consume] - Группа прочитает сообщение, при этом только 1 consume прочитает сообщение.
    //////////////////////////////////////////


    //@KafkaListener(id = Listener7, topicPartitions = @TopicPartition(topic = topic2, partitions = {"0"}), containerFactory = "singleFactory")
    @KafkaListener(id = Listener7, topics = {topic2}, containerFactory = "singleFactory", groupId = Group3)
    public void consume7(PersonDto dto) {
        consumeWork(Group3, Listener7, dto, topic2, null);
    }

    @KafkaListener(id = Listener8, topics = {topic2}, containerFactory = "singleFactory", groupId = Group3)
    public void consume8(PersonDto dto) {
        consumeWork(Group3, Listener8, dto, topic2, null);
    }

    @KafkaListener(id = Listener9, topics = {topic2}, containerFactory = "singleFactory", groupId = Group3)
    public void consume9(PersonDto dto) {
        consumeWork(Group3, Listener9, dto, topic2, null);
    }
    //////////////////////////////////////////
    //1message1topic2partition - 1group3listeners for topic - each partition listen only 1 listener (end listener don't work)
    ////////
    //6message-1topic-1partition - 1group[2consume] - Группа настроена только на топик. Группа прочитает сообщение, при этом только 1 consume прочитает сообщение.
    //6message-1topic-2partition - 1group[3consume] - Группа настроена только на топик. 2 consume (каждый) настроится на одну партицию и будут вычитывать только из нее. Последний consume не будет ниоткуда вычитывать.
    //////////////////////////////////////////



    private void consumeWork(String group, String listener, PersonDto dto, String topic, Integer partition) {
        WorkUtils.setTransportInfoReceive(topic, partition, dto, listener);
        log.info("Group: {}; listener: {}; consumer: {}", group, listener, WorkUtils.writeValueAsString(objectMapper, dto));
        try {
            PersonDto personDto = objectMapper.readValue(WorkUtils.writeValueAsString(objectMapper, dto), PersonDto.class);
            //log.info("object: {}", personDto);
            WorkUtils.personDtoMap.put(personDto.getId(), personDto);
        }
        catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
