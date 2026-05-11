package eu.maximzin.productMicroservice.service.dto;

import java.math.BigDecimal;

public record CreatedProductDto(
        String title,
        BigDecimal price,
        Integer quantity
) {}
