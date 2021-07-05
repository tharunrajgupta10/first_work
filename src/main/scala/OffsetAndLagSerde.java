import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class OffsetAndLagSerde implements Serde<OffsetAndLag> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<OffsetAndLag> serializer() {
        return new Serializer<OffsetAndLag>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public byte[] serialize(String topic, OffsetAndLag data) {
                String countAndSum = data.getOffset() + ":" + data.getLag();
                return countAndSum.getBytes();
            }

            @Override
            public void close() {
            }
        };
    }


    @Override
    public Deserializer<OffsetAndLag> deserializer() {
        return new Deserializer<OffsetAndLag>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public OffsetAndLag deserialize(String topic, byte[] countAndSum) {
                String countAndSumStr = new String(countAndSum);
                Long offset = Long.valueOf(countAndSumStr.split(":")[0]);
                Long lag = Long.valueOf(countAndSumStr.split(":")[1]);

                OffsetAndLag countAndSumObject = new OffsetAndLag(offset, lag);

                return countAndSumObject;
            }

            @Override
            public void close() {
            }
        };
    }
}