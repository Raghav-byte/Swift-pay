package com.swiftpay.gateway.exception;

public class UnsupportedCurrencyException extends SwiftPayException {

    public UnsupportedCurrencyException(String currency) {
        super(422, "UNSUPPORTED_CURRENCY", "Currency not supported: " + currency);
    }
}
