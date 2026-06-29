package com.swiftpay.gateway.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class HealthResponseDTO {

    private String status;
    private String db;
    private String redis;
    private String kafka;
}
