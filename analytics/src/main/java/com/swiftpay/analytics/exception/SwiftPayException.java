package com.swiftpay.analytics.exception;

public class SwiftPayException extends Exception {

    private final int httpStatus;
    private final String code;

    public SwiftPayException(int httpStatus, String code, String message) {
        super(message);
        this.httpStatus = httpStatus;
        this.code = code;
    }

    public SwiftPayException(int httpStatus, String code, String message, Throwable cause) {
        super(message, cause);
        this.httpStatus = httpStatus;
        this.code = code;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public String getCode() {
        return code;
    }
}
