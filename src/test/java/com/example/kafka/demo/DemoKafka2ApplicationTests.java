package com.example.kafka.demo;

import com.example.kafka.demo.dto.DocDto;
import com.example.kafka.demo.dto.PersonDto;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;

@RunWith(SpringRunner.class)

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class DemoKafka2ApplicationTests {

	@Autowired
	private TestRestTemplate restTemplate;

	@LocalServerPort
	int randomServerPort;

	@Test
	public void testSendToKafka() throws URISyntaxException
	{
		final String baseUrl = "http://localhost:"+randomServerPort+"/kafka/publishperson/";
		URI uri = new URI(baseUrl);

		DocDto docDto1 = new DocDto("SN333", 7777777);
		PersonDto personDto1 = new PersonDto(27l, LocalDateTime.now(), LocalDateTime.now(), "Jon", "Nechiporuk", docDto1);

		HttpHeaders headers = new HttpHeaders();
		HttpEntity<PersonDto> request = new HttpEntity<>(personDto1, headers);
		ResponseEntity<PersonDto> result = this.restTemplate.postForEntity(uri, request, PersonDto.class);
		Assert.assertEquals(personDto1, result.getBody());
	}

}
