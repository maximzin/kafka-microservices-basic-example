package eu.maximzin.email_notification_microservice.exception;

import java.time.LocalDateTime;

public class RetryableException  extends BusinessException {

    public RetryableException(String httpStatus, String message, LocalDateTime timestamp) {
        super(httpStatus, message, timestamp);
    }

}
