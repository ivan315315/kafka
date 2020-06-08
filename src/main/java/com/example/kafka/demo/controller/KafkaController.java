package com.example.kafka.demo.controller;

import com.example.kafka.demo.dto.PersonDto;
import com.example.kafka.demo.service.KafkaServicePersonDto;
import com.example.kafka.demo.utils.WorkUtils;
import org.slf4j.Logger;
import org.springframework.web.bind.annotation.*;

import static org.slf4j.LoggerFactory.getLogger;

@RestController
@RequestMapping("kafka")
public class KafkaController {

    private static final Logger log = getLogger(KafkaController.class);

    private final KafkaServicePersonDto kafkaServicePersonDto;

    public KafkaController(KafkaServicePersonDto kafkaServicePersonDto) {
        this.kafkaServicePersonDto = kafkaServicePersonDto;
    }

    @PostMapping (("/publishperson"))
    public PersonDto sendPerson(@RequestBody PersonDto dto) {
        log.info("send: {}", dto);
        synchronized (this) {
            WorkUtils.personDtoMap.put(dto.getId(), null);
            kafkaServicePersonDto.send(dto);
            while (WorkUtils.personDtoMap.get(dto.getId()) == null) {
                try {
                    Thread.sleep(1);
                }
                catch (InterruptedException e) {
                }
            }
        }
        return WorkUtils.personDtoMap.get(dto.getId());
    }
}
//http://localhost:8082/kafka/publishperson
/* {
	"id":7,
	"created":"2020-06-05 18:00:06.789",
	"updated":"2020-06-05 18:00:06.789",
	"firstName":"Jon",
	"lastName":"Nechiporuk",
	"docDto":
	{
		"serial":"SN333",
		"number":7777777
	}
}*/