package com.swiftpay.gateway.exception;

import com.swiftpay.gateway.dto.ErrorResponseDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(SwiftPayException.class)
    public ResponseEntity<ErrorResponseDTO> handleSwiftPayException(SwiftPayException ex, WebRequest request) {
        ErrorResponseDTO body = new ErrorResponseDTO(
                ex.getCode(),
                ex.getMessage(),
                OffsetDateTime.now(ZoneOffset.UTC),
                request.getDescription(false).replace("uri=", ""));
        return ResponseEntity.status(ex.getHttpStatus()).body(body);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponseDTO> handleValidation(MethodArgumentNotValidException ex, WebRequest request) {
        String message = ex.getBindingResult().getFieldErrors().stream()
                .map(e -> e.getField() + ": " + e.getDefaultMessage())
                .collect(Collectors.joining(", "));
        ErrorResponseDTO body = new ErrorResponseDTO(
                "VALIDATION_ERROR",
                message,
                OffsetDateTime.now(ZoneOffset.UTC),
                request.getDescription(false).replace("uri=", ""));
        return ResponseEntity.badRequest().body(body);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponseDTO> handleGeneric(Exception ex, WebRequest request) {
        logger.error("Unexpected error", ex);
        ErrorResponseDTO body = new ErrorResponseDTO(
                "INTERNAL_ERROR",
                "An unexpected error occurred",
                OffsetDateTime.now(ZoneOffset.UTC),
                request.getDescription(false).replace("uri=", ""));
        return ResponseEntity.internalServerError().body(body);
    }
}
