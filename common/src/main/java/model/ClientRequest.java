package model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Запрос клиента
 */
@Data
public class ClientRequest {
    @JsonProperty("user_id")
    private String userId;
    private RequestType type;
    // Строка поиска, только для SEARCH
    private String query;
    private String timestamp;
}
