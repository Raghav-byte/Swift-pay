package com.swiftpay.ledger.controller;

import com.swiftpay.ledger.dto.DependencyHealthDTO;
import com.swiftpay.ledger.dto.HealthResponseDTO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.swiftpay.ledger.event.PaymentInitiatedEvent;
import org.apache.kafka.clients.consumer.Consumer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;

@RestController
@RequestMapping("/v1/health")
public class HealthController {

    private static final Logger logger = LoggerFactory.getLogger(HealthController.class);

    private final DataSource dataSource;
    private final ConsumerFactory<String, PaymentInitiatedEvent> consumerFactory;

    public HealthController(
            DataSource dataSource,
            ConsumerFactory<String, PaymentInitiatedEvent> consumerFactory) {
        this.dataSource = dataSource;
        this.consumerFactory = consumerFactory;
    }

    @Operation(
            summary = "Overall health check for ledger service",
            responses = {
                    @ApiResponse(responseCode = "200", description = "All dependencies UP"),
                    @ApiResponse(responseCode = "503", description = "One or more dependencies DOWN")
            })
    @GetMapping
    public ResponseEntity<HealthResponseDTO> health() {
        String db = checkDb();
        String kafka = checkKafka();
        String overall = (db.equals("UP") && kafka.equals("UP")) ? "UP" : "DOWN";
        int code = overall.equals("UP") ? 200 : 503;
        if (!"UP".equals(overall)) {
            logger.warn("Health check degraded: overall={}, db={}, kafka={}", overall, db, kafka);
        }
        return ResponseEntity.status(code).body(new HealthResponseDTO(overall, db, kafka));
    }

    @Operation(
            summary = "Database health check",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Database UP"),
                    @ApiResponse(responseCode = "503", description = "Database DOWN")
            })
    @GetMapping("/db")
    public ResponseEntity<DependencyHealthDTO> healthDb() {
        return dependencyResponse(checkDb());
    }

    @Operation(
            summary = "Kafka health check",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Kafka UP"),
                    @ApiResponse(responseCode = "503", description = "Kafka DOWN")
            })
    @GetMapping("/kafka")
    public ResponseEntity<DependencyHealthDTO> healthKafka() {
        return dependencyResponse(checkKafka());
    }

    private ResponseEntity<DependencyHealthDTO> dependencyResponse(String status) {
        int code = "UP".equals(status) ? 200 : 503;
        return ResponseEntity.status(code).body(new DependencyHealthDTO(status));
    }

    private String checkDb() {
        try (Connection c = dataSource.getConnection()) {
            return c.isValid(1) ? "UP" : "DOWN";
        } catch (Exception e) {
            return "DOWN";
        }
    }

    private String checkKafka() {
        try (Consumer<String, PaymentInitiatedEvent> consumer = consumerFactory.createConsumer()) {
            consumer.listTopics(Duration.ofSeconds(3));
            return "UP";
        } catch (Exception e) {
            logger.debug("Kafka health check failed", e);
            return "DOWN";
        }
    }
}
