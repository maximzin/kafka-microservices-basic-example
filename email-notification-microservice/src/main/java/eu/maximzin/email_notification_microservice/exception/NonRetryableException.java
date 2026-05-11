package eu.maximzin.email_notification_microservice.exception;

import java.time.LocalDateTime;

public class NonRetryableException extends BusinessException {

    public NonRetryableException(String httpStatus, String message, LocalDateTime timestamp) {
        super(httpStatus, message, timestamp);
    }

}
