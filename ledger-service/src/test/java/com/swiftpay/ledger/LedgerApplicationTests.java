package com.swiftpay.ledger;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class LedgerApplicationTests {

    @Test
    void applicationClassLoads() {
        assertNotNull(LedgerApplication.class);
    }
}
