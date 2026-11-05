package model;

import lombok.Data;

/**
 * Цена товара
 */
@Data
public class Price {
    private Double amount;
    private String currency;
}
