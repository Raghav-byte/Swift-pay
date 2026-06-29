package com.swiftpay.ledger.exception;

import java.util.UUID;

public class AccountNotFoundException extends SwiftPayException {

    public AccountNotFoundException(UUID ownerId) {
        super(404, "ACCOUNT_NOT_FOUND", "Account not found for owner: " + ownerId);
    }
}
