package util;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

/***
 * Класс сериализации сообщений
 */
@Slf4j
public class JsonSerializer {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String toJsonString(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            log.error("Ошибка при сериализации: {}", e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    public static byte[] toJsonBytes(Object obj) {
        try {
            return objectMapper.writeValueAsBytes(obj);
        } catch (Exception e) {
            log.error("Ошибка при сериализации: {}", e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }
}
