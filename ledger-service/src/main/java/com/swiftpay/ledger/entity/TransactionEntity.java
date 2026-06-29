package com.swiftpay.ledger.entity;

import com.swiftpay.ledger.enums.TransactionStatusEnum;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "sp_transactions")
@AttributeOverride(name = "dateCreated", column = @Column(name = "created_at", nullable = false, updatable = false))
@AttributeOverride(name = "dateModified", column = @Column(name = "updated_at"))
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class TransactionEntity extends AuditableBaseEntity {

    @Column(name = "sender_id", nullable = false)
    private UUID senderId;

    @Column(name = "receiver_id", nullable = false)
    private UUID receiverId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 10)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private TransactionStatusEnum status;

    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;
}
