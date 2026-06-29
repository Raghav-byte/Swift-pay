package com.swiftpay.analytics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class AnalyticsApplicationTests {

    @Test
    void applicationClassLoads() {
        assertNotNull(AnalyticsApplication.class);
    }
}
