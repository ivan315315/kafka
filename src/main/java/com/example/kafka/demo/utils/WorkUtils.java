package com.example.kafka.demo.utils;

import com.example.kafka.demo.dto.AbstractDto;
import com.example.kafka.demo.dto.PersonDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class WorkUtils {

    public static Map<Long, PersonDto> personDtoMap = new HashMap<>();

    public static String writeValueAsString(ObjectMapper objectMapper,  AbstractDto dto) {
        try {
            return objectMapper.writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new RuntimeException("Writing value to JSON failed: " + dto.toString());
        }
    }

    public static void setTransportInfoSend(String topic, Integer partition, PersonDto personDto) {
        String topicInfo = "ACTION-[send]"
                + " topic-[" + topic + "]"
                + " partitionSend-[" + partition + "]";
        personDto.setKafkaTransportInfo(topicInfo);
    }

    public static void setTransportInfoReceive(String topic, Integer partition, PersonDto personDto, String listener) {
        String topicInfo = personDto.getKafkaTransportInfo()
                //+ '\n'
                + " ACTION-[receive]"
                + " topic-[" + topic + "]"
                + " partitionConfigRead-[" + partition + "]"
                + " listener-[" + listener + "]";
        personDto.setKafkaTransportInfo(topicInfo);
    }
}
