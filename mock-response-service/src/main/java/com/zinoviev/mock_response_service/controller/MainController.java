package com.zinoviev.mock_response_service.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class MainController {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @GetMapping("/200")
    public ResponseEntity<String> getSuccessResponse() {
        LOGGER.info("Received success request");
        return ResponseEntity.ok("SUCCESS!");
    }

    @GetMapping("/500")
    public ResponseEntity<String> getFailedResponse() {
        LOGGER.info("Received failed request");
        return ResponseEntity.ok("FAILED!");
    }


}
