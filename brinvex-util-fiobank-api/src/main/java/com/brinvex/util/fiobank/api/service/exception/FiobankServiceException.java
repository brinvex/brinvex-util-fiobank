package com.brinvex.util.fiobank.api.service.exception;

public class FiobankServiceException extends RuntimeException {

    public FiobankServiceException(String message) {
        super(message);
    }

    public FiobankServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}
