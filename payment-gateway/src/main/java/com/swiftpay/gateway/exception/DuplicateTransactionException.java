package com.swiftpay.gateway.exception;

public class DuplicateTransactionException extends SwiftPayException {

    public DuplicateTransactionException(String transactionId) {
        super(409, "DUPLICATE_TRANSACTION", "Transaction " + transactionId + " already processed");
    }
}
