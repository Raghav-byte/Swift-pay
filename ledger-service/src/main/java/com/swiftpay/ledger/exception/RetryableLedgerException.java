package com.swiftpay.ledger.exception;

/**
 * Signals a transient ledger failure so the Kafka consumer can retry or route to DLQ.
 */
public class RetryableLedgerException extends RuntimeException {

    public RetryableLedgerException(String message, Throwable cause) {
        super(message, cause);
    }
}
