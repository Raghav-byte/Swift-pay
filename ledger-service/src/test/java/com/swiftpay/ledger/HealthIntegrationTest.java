package com.swiftpay.ledger;

import com.swiftpay.ledger.dto.DependencyHealthDTO;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.assertj.core.api.Assertions.assertThat;

class HealthIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void healthKafka_reportsUpWhenBrokerReachable() {
        ResponseEntity<DependencyHealthDTO> v1 = restTemplate.getForEntity(
                "/v1/health/kafka", DependencyHealthDTO.class);
        ResponseEntity<DependencyHealthDTO> alias = restTemplate.getForEntity(
                "/health/kafka", DependencyHealthDTO.class);

        assertThat(v1.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(v1.getBody()).isNotNull();
        assertThat(v1.getBody().getStatus()).isEqualTo("UP");

        assertThat(alias.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(alias.getBody()).isNotNull();
        assertThat(alias.getBody().getStatus()).isEqualTo("UP");
    }
}
