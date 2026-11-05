package util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

/***
 * Класс десериализации сообщений
 */
@Slf4j
public class JsonDeserializer {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static <T> T fromStringToObject(String jsonString, Class<T> objectClass) {
        try {
            return objectMapper.readValue(jsonString, objectClass);
        } catch (Exception e) {
            log.error("Ошибка при десериализации: {}", e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    public static <T> T fromBytesToObject(byte[] jsonBytes, Class<T> objectClass) {
        try {
            return objectMapper.readValue(jsonBytes, objectClass);
        } catch (Exception e) {
            log.error("Ошибка при десериализации: {}", e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    public static <T> T fromBytesToObject(byte[] jsonBytes, TypeReference<T> typeReference) {
        try {
            return objectMapper.readValue(jsonBytes, typeReference);
        } catch (Exception e) {
            log.error("Ошибка при десериализации: {}", e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }
}
