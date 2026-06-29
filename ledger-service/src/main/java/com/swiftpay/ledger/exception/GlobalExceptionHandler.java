package com.swiftpay.ledger.exception;

import com.swiftpay.ledger.dto.ErrorResponseDTO;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(SwiftPayException.class)
    public ResponseEntity<ErrorResponseDTO> handleSwiftPayException(SwiftPayException ex, HttpServletRequest request) {
        return ResponseEntity.status(ex.getHttpStatus())
                .body(new ErrorResponseDTO(
                        ex.getCode(),
                        ex.getMessage(),
                        OffsetDateTime.now(ZoneOffset.UTC),
                        request.getRequestURI()));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponseDTO> handleValidationException(
            MethodArgumentNotValidException ex, HttpServletRequest request) {
        String message = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .reduce((a, b) -> a + "; " + b)
                .orElse("Validation failed");
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new ErrorResponseDTO(
                        "VALIDATION_ERROR",
                        message,
                        OffsetDateTime.now(ZoneOffset.UTC),
                        request.getRequestURI()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponseDTO> handleGenericException(Exception ex, HttpServletRequest request) {
        logger.error("Unexpected error on path={}", request.getRequestURI(), ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponseDTO(
                        "INTERNAL_ERROR",
                        "An unexpected error occurred",
                        OffsetDateTime.now(ZoneOffset.UTC),
                        request.getRequestURI()));
    }
}
