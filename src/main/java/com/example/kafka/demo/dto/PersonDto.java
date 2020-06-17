package com.example.kafka.demo.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;


//@EqualsAndHashCode(callSuper = true)
@Data
public class PersonDto extends AbstractDto {
    private String firstName;
    private String lastName;
    private DocDto docDto;

    public PersonDto() {
    }

    public PersonDto(Long id, LocalDateTime created, LocalDateTime updated, String firstName, String lastName, DocDto docDto) {
        super(id, created, updated);
        this.firstName = firstName;
        this.lastName = lastName;
        this.docDto = docDto;
    }

    public String getKafkaTransportInfo() {
        return kafkaTransportInfo;
    }

    public void setKafkaTransportInfo(String kafkaTransportInfo) {
        this.kafkaTransportInfo = kafkaTransportInfo;
    }
}
