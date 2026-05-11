package eu.maximzin.productMicroservice.service;

import eu.maximzin.productMicroservice.service.dto.CreatedProductDto;

import java.util.concurrent.ExecutionException;

public interface ProductService {

    String createProduct(CreatedProductDto createdProductDto) throws ExecutionException, InterruptedException;

}
