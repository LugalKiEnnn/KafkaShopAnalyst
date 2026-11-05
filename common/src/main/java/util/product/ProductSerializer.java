package util.product;

import model.Product;
import org.apache.kafka.common.serialization.Serializer;
import util.JsonSerializer;

public class ProductSerializer implements Serializer<Product> {
    @Override
    public byte[] serialize(String topic, Product data) {
        if (data == null) return null;

        return JsonSerializer.toJsonBytes(data);
    }
}
