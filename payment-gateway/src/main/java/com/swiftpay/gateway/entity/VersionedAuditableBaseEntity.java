package com.swiftpay.gateway.entity;

import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Version;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@MappedSuperclass
public abstract class VersionedAuditableBaseEntity extends AuditableBaseEntity {

    @Version
    @Column(name = "version", nullable = false)
    private int dbVersion;
}
