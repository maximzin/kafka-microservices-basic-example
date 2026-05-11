package eu.maximzin.email_notification_microservice.exception;

import java.time.LocalDateTime;

public abstract class BusinessException extends RuntimeException {

    private String HttpStatus;
    private LocalDateTime timestamp;

    public BusinessException(String httpStatus, String message, LocalDateTime timestamp) {
        super(message);
        this.HttpStatus = httpStatus;
        this.timestamp = timestamp;
    }

    public BusinessException(Throwable cause, String httpStatus, LocalDateTime timestamp) {
        super(cause);
        HttpStatus = httpStatus;
        this.timestamp = timestamp;
    }
}
