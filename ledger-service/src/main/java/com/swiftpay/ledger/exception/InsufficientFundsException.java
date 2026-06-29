package com.swiftpay.ledger.exception;

public class InsufficientFundsException extends SwiftPayException {

    public InsufficientFundsException() {
        super(422, "INSUFFICIENT_FUNDS", "Sender has insufficient balance");
    }
}
