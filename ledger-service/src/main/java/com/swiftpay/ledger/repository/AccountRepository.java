package com.swiftpay.ledger.repository;

import com.swiftpay.ledger.entity.AccountEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface AccountRepository extends JpaRepository<AccountEntity, UUID> {

    Optional<AccountEntity> findByOwnerId(UUID ownerId);

    @Query(value = "SELECT * FROM sp_accounts WHERE owner_id=:ownerId FOR UPDATE", nativeQuery = true)
    Optional<AccountEntity> findByOwnerIdForUpdate(@Param("ownerId") UUID ownerId);
}
