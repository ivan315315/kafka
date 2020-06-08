package com.example.kafka.demo.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

//@EqualsAndHashCode(callSuper = true)
@Data
public class DocDto {
    private String serial;
    private Integer number;

    public DocDto() {
    }

    public DocDto(String serial, Integer number) {
        this.serial = serial;
        this.number = number;
    }
}
