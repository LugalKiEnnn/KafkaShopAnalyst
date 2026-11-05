package util.bannedProduct;

import model.BannedProduct;
import org.apache.kafka.common.serialization.Serializer;
import util.JsonSerializer;

public class BannedProductSerializer implements Serializer<BannedProduct> {
    @Override
    public byte[] serialize(String topic, BannedProduct data) {
        if (data == null) return null;

        return JsonSerializer.toJsonBytes(data);
    }
}
