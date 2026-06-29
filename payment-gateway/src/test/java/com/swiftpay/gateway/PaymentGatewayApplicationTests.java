package com.swiftpay.gateway;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class PaymentGatewayApplicationTests {

    @Test
    void applicationClassLoads() {
        assertNotNull(PaymentGatewayApplication.class);
    }
}
