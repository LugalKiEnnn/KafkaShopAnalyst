package util.bannedProduct;

import model.BannedProduct;
import org.apache.kafka.common.serialization.Serdes;

public class BannedProductSerdes extends Serdes.WrapperSerde<BannedProduct> {
    public BannedProductSerdes() {
        super(new BannedProductSerializer(), new BannedProductDeserializer());
    }
}
