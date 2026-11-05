package model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Запрет на продажу товара
 */
@Data
public class BannedProduct {
    @JsonProperty("product_id")
    private String productId;
    // Причина запрета
    private String reason;
    // Время добавления запрета
    @JsonProperty("banned_at")
    private String bannedAt;
}
