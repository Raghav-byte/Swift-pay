package com.swiftpay.gateway.controller;

import com.swiftpay.gateway.dto.PaymentRequestDTO;
import com.swiftpay.gateway.dto.PaymentResponseDTO;
import com.swiftpay.gateway.exception.SwiftPayException;
import com.swiftpay.gateway.service.PaymentService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1")
public class PaymentController {

    private static final Logger logger = LoggerFactory.getLogger(PaymentController.class);

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @Operation(
            summary = "Initiate a P2P payment",
            responses = {
                    @ApiResponse(responseCode = "202", description = "Payment accepted and queued"),
                    @ApiResponse(responseCode = "400", description = "Validation failure"),
                    @ApiResponse(responseCode = "409", description = "Duplicate transaction"),
                    @ApiResponse(responseCode = "422", description = "Insufficient funds or unsupported currency"),
                    @ApiResponse(responseCode = "500", description = "Internal server error")
            })
    @PostMapping("/payments")
    public ResponseEntity<PaymentResponseDTO> initiatePayment(@Valid @RequestBody PaymentRequestDTO request)
            throws SwiftPayException {

        logger.info("Payment request received: transactionId={}, senderId={}, receiverId={}, amount={}",
                request.getTransactionId(), request.getSenderId(), request.getReceiverId(),
                request.getAmount());

        PaymentResponseDTO response = paymentService.initiatePayment(request);

        logger.info("Payment accepted: transactionId={}, status={}", response.getTransactionId(), response.getStatus());

        return ResponseEntity.accepted().body(response);
    }
}
