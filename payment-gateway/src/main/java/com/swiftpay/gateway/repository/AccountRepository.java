package com.swiftpay.gateway.repository;

import com.swiftpay.gateway.entity.AccountEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface AccountRepository extends JpaRepository<AccountEntity, UUID> {

    Optional<AccountEntity> findByOwnerId(UUID ownerId);
}
