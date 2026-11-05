package model;

import lombok.Data;

/**
 * Остатки товара
 */
@Data
public class Stock {
    private Integer available;
    private Integer reserved;
}
