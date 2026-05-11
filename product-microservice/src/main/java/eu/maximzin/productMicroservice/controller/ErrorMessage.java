package eu.maximzin.productMicroservice.controller;

import java.time.LocalDateTime;

public class ErrorMessage extends RuntimeException {

    private String status;
    private LocalDateTime timestamp;

    public ErrorMessage(String message, String status, LocalDateTime timestamp) {
        super(message);
        this.status = status;
        this.timestamp = timestamp;
    }
}
