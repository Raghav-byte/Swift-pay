package com.swiftpay.gateway.controller;

import com.swiftpay.gateway.dto.DependencyHealthDTO;
import com.swiftpay.gateway.dto.HealthResponseDTO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/v1/health")
public class HealthController {

    private static final Logger logger = LoggerFactory.getLogger(HealthController.class);

    private final DataSource dataSource;
    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<?, ?> kafkaTemplate;

    public HealthController(
            DataSource dataSource,
            StringRedisTemplate redisTemplate,
            KafkaTemplate<?, ?> kafkaTemplate) {
        this.dataSource = dataSource;
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Operation(
            summary = "Overall health check for payment gateway",
            responses = {
                    @ApiResponse(responseCode = "200", description = "All dependencies UP"),
                    @ApiResponse(responseCode = "503", description = "One or more dependencies DOWN")
            })
    @GetMapping
    public ResponseEntity<HealthResponseDTO> health() {
        String db = checkDb();
        String redis = checkRedis();
        String kafka = checkKafka();
        String overall = (db.equals("UP") && redis.equals("UP") && kafka.equals("UP")) ? "UP" : "DOWN";
        int code = overall.equals("UP") ? 200 : 503;
        logger.debug("Health check: overall={}, db={}, redis={}, kafka={}", overall, db, redis, kafka);
        if (!"UP".equals(overall)) {
            logger.warn("Health check degraded: overall={}, db={}, redis={}, kafka={}", overall, db, redis, kafka);
        }
        return ResponseEntity.status(code).body(new HealthResponseDTO(overall, db, redis, kafka));
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
            summary = "Redis health check",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Redis UP"),
                    @ApiResponse(responseCode = "503", description = "Redis DOWN")
            })
    @GetMapping("/redis")
    public ResponseEntity<DependencyHealthDTO> healthRedis() {
        return dependencyResponse(checkRedis());
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

    private String checkRedis() {
        try {
            redisTemplate.opsForValue().get("__health__");
            return "UP";
        } catch (Exception e) {
            return "DOWN";
        }
    }

    private String checkKafka() {
        if (kafkaTemplate == null) {
            return "DOWN";
        }
        Map<String, Object> props = new HashMap<>(kafkaTemplate.getProducerFactory().getConfigurationProperties());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        try (AdminClient admin = AdminClient.create(props)) {
            admin.describeCluster().clusterId().get();
            return "UP";
        } catch (Exception e) {
            logger.debug("Kafka health check failed", e);
            return "DOWN";
        }
    }
}