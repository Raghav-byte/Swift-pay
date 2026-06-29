package com.swiftpay.gateway.utils;

import com.swiftpay.gateway.dto.PaymentResponseDTO;
import com.swiftpay.gateway.entity.TransactionEntity;
import com.swiftpay.gateway.enums.TransactionStatusEnum;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public final class ResponseUtil {

    private ResponseUtil() {
    }

    public static PaymentResponseDTO paymentAccepted(TransactionEntity tx) {
        return new PaymentResponseDTO(
                tx.getId(),
                TransactionStatusEnum.PENDING.name(),
                "Payment accepted",
                OffsetDateTime.now(ZoneOffset.UTC));
    }
}
