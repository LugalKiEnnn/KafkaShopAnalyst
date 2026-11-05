package util.bannedProduct;

import model.BannedProduct;
import org.apache.kafka.common.serialization.Deserializer;
import util.JsonDeserializer;

public class BannedProductDeserializer implements Deserializer<BannedProduct> {
    @Override
    public BannedProduct deserialize(String s, byte[] data) {
        if (data == null) {
            return null;
        }

        return JsonDeserializer.fromBytesToObject(data, BannedProduct.class);
    }
}
