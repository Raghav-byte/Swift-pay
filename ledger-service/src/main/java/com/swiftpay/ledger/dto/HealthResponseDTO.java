package com.swiftpay.ledger.dto;

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
    private String kafka;
}
