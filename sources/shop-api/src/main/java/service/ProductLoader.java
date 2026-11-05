package service;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import model.Product;
import util.JsonDeserializer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Чтение товаров из файла
 */
@Slf4j
public class ProductLoader {

    /**
     * Прочитать товары из json файла
     * @param fileName путь к файлу
     * @return список товаров
     */
    public static List<Product> loadProducts(String fileName) {
        try {
            byte[] bytes = Files.readAllBytes(Path.of(fileName));
            List<Product> products = JsonDeserializer.fromBytesToObject(bytes, new TypeReference<List<Product>>() {});

            log.info("Прочитано товаров из {}: {}", fileName, products.size());

            return products;
        } catch (Exception e) {
            log.error("Ошибка чтения товаров из файла: {}", e.getMessage());
            throw new IllegalStateException(e);
        }
    }
}
