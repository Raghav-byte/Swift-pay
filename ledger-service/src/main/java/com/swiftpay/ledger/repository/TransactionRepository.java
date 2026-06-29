package com.swiftpay.ledger.repository;

import com.swiftpay.ledger.entity.TransactionEntity;
import com.swiftpay.ledger.enums.TransactionStatusEnum;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface TransactionRepository extends JpaRepository<TransactionEntity, UUID> {

    @Query(value = """
            SELECT * FROM sp_transactions
            WHERE sender_id = :userId OR receiver_id = :userId
            ORDER BY created_at DESC
            LIMIT :size OFFSET :offset
            """, nativeQuery = true)
    List<TransactionEntity> findByUserId(
            @Param("userId") UUID userId,
            @Param("offset") int offset,
            @Param("size") int size);

    @Query(value = """
            SELECT COUNT(*) FROM sp_transactions
            WHERE sender_id = :userId OR receiver_id = :userId
            """, nativeQuery = true)
    long countByUserId(@Param("userId") UUID userId);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "UPDATE sp_transactions SET status = :#{#status.name()}, updated_at = NOW() WHERE id = :id",
            nativeQuery = true)
    int updateStatus(@Param("id") UUID id, @Param("status") TransactionStatusEnum status);
}
