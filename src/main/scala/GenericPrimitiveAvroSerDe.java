import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.Map;

public class GenericPrimitiveAvroSerDe<T> implements Serde<T> {
    private final Serde<Object> inner;

    /**
     * Constructor used by Kafka Streams.
     */
    public GenericPrimitiveAvroSerDe() {
        inner = Serdes.serdeFrom(new KafkaAvroSerializer(), new KafkaAvroDeserializer());
    }

    public GenericPrimitiveAvroSerDe(SchemaRegistryClient client) {
        this(client, Collections.emptyMap());
    }

    public GenericPrimitiveAvroSerDe(SchemaRegistryClient client, Map<String, ?> props) {
        inner = Serdes.serdeFrom(new KafkaAvroSerializer(client), new KafkaAvroDeserializer(client, props));
    }

    @Override
    public void configure(final Map<String, ?> serdeConfig, final boolean isSerdeForRecordKeys) {
        inner.serializer().configure(serdeConfig, isSerdeForRecordKeys);
        inner.deserializer().configure(serdeConfig, isSerdeForRecordKeys);
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        inner.serializer().close();
        inner.deserializer().close();

    }

    @SuppressWarnings("unchecked")
    @Override
    public Serializer<T> serializer() {
        // TODO Auto-generated method stub
        Object obj = inner.serializer();
        return (Serializer<T>) obj;

    }

    @SuppressWarnings("unchecked")
    @Override
    public Deserializer<T> deserializer() {
        // TODO Auto-generated method stub
        Object obj = inner.deserializer();
        return (Deserializer<T>) obj;

    }
}

