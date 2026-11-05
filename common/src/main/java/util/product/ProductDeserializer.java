package util.product;

import model.Product;
import org.apache.kafka.common.serialization.Deserializer;
import util.JsonDeserializer;

public class ProductDeserializer implements Deserializer<Product> {
    @Override
    public Product deserialize(String s, byte[] data) {
        if (data == null) {
            return null;
        }

        return JsonDeserializer.fromBytesToObject(data, Product.class);
    }
}
