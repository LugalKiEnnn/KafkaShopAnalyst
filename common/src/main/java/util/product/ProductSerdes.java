package util.product;

import model.Product;
import org.apache.kafka.common.serialization.Serdes;

public class ProductSerdes extends Serdes.WrapperSerde<Product> {
    public ProductSerdes() {
        super(new ProductSerializer(), new ProductDeserializer());
    }
}
