import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.json.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class OffsetAndLagApp {

    public static KafkaStreams start() {
        String schemaRegistryURL = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", Application.SCHEMA_REGISTRY_URL);
        String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", Application.BOOTSTRAP_SERVER);

        final String OFFSET_CONSUMER_GROUP_ID = System.getenv().getOrDefault("OFFSET_CONSUMER_GROUP_ID", "consumer-offset3");

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, OFFSET_CONSUMER_GROUP_ID);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, System.getenv().getOrDefault("AUTO_OFFSET_RESET_CONFIG", "earliest"));
        config.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        config.put(StreamsConfig.STATE_DIR_CONFIG, System.getenv().getOrDefault("STATE_STORE_LOCATION", "/tmp/kafka-streams"));
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class);


        StreamsBuilder builder = new StreamsBuilder();


        Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();
        Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistryURL);

        genericAvroSerde.configure(serdeConfig, false);

        final GenericPrimitiveAvroSerDe<Object> keyGenericAvroSerde = new GenericPrimitiveAvroSerDe<Object>();
        keyGenericAvroSerde.configure(serdeConfig, true);


//        Builds a stream for the topic using the provided serdes
        KStream<Object, GenericRecord> stream = builder.stream(System.getenv().getOrDefault("CONSUMER_OFFSETS_OUTPUT_TOPIC", "consumerOffsetsTest3"), Consumed.with(keyGenericAvroSerde, genericAvroSerde));

        createStream(stream, OFFSET_CONSUMER_GROUP_ID);

//        stream.print(Printed.toSysOut());

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
//        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
//            // here you should examine the throwable/exception and perform an appropriate action!
//        });
        streams.start();

        return streams;
    }

    static KStream<Object, GenericRecord> createStream(KStream<Object, GenericRecord> stream, String OFFSET_CONSUMER_GROUP_ID) {
        stream
                .filter((k,v) -> !v.get("topic").equals(System.getenv().getOrDefault("CONSUMER_OFFSETS_OUTPUT_TOPIC", "consumerOffsetsTest3")) || !v.get("topic").equals(OFFSET_CONSUMER_GROUP_ID + "-offset_lag-repartition"))
                .groupBy((k,v) -> v.get("topic").toString() + ":" + v.get("partition").toString() + ":" + v.get("consumerGroup").toString())
                .windowedBy(TimeWindows.of(Duration.ofHours(1)))
                .aggregate(
                        () -> new OffsetAndLag(0L, Long.MAX_VALUE),
                        (k, v, a) -> new OffsetAndLag(Math.max((Long) v.get("offset"), a.getOffset()), Math.min(0, a.getLag())),
                        Materialized.<String, OffsetAndLag, WindowStore<Bytes, byte[]>>as( "offset_lag")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new OffsetAndLagSerde())
                )
        ;
        return stream;
    }


    static ResponseEntity<String> getConsumersInfo(KafkaStreams streams, ConsumersInfoModel consumersInfoModel) throws InterruptedException {

        String storeName = "offset_lag";

        waitUntilStoreIsQueryable(storeName, QueryableStoreTypes.windowStore(), streams);

        ReadOnlyWindowStore<String, Double> windowStore = streams.store(storeName, QueryableStoreTypes.windowStore());
//
        Instant timeFrom = Instant.ofEpochMilli(Long.parseLong(consumersInfoModel.getStartTime()));
        Instant timeTo = Instant.ofEpochMilli(Long.parseLong(consumersInfoModel.getEndTime()));
        KeyValueIterator<Windowed<String>, Double> iterator = windowStore.fetchAll(timeFrom, timeTo);
        ArrayList<JSONObject> output = new ArrayList<>();

        HashMap<HashMap<String, String>, Long> consumedMap = new HashMap<>();
        HashMap<HashMap<String, String>, Long> minLagMap = new HashMap<>();


        while (iterator.hasNext()) {

            KeyValue<Windowed<String>, Double> next = iterator.next();
            Windowed<String> windowTimestamp = next.key;
            if (windowTimestamp.key().startsWith(consumersInfoModel.getTopicName())) {
                HashMap<String, String> consumerGroupTimestamp = new HashMap<>();


                String consumerGroup = windowTimestamp.key().split(":")[windowTimestamp.key().split(":").length - 1];

                String startTime = windowTimestamp.window().startTime().toString();

                consumerGroupTimestamp.put(consumerGroup, startTime);
                Long consumed = Long.parseLong(String.valueOf(next.value).split(":")[0]);
                Long minLag = Long.parseLong(String.valueOf(next.value).split(":")[1]);

                if (consumedMap.containsKey(consumerGroupTimestamp)) {
                    consumedMap.replace(consumerGroupTimestamp, consumedMap.getOrDefault(consumerGroupTimestamp, 0L) + consumed);
                }
                else {
                    consumedMap.put(consumerGroupTimestamp, 0L + consumed);
                }

                if (minLagMap.containsKey(consumerGroupTimestamp)) {
                    minLagMap.replace(consumerGroupTimestamp, minLagMap.getOrDefault(consumerGroupTimestamp, 0L) + minLag);
                }
                else {
                    minLagMap.put(consumerGroupTimestamp, 0L + minLag);
                }
//                consumedMap.put(consumerGroupTimestamp, consumedMap.getOrDefault(consumerGroupTimestamp, 0L) + consumed);
//                minLagMap.put(consumerGroupTimestamp, minLagMap.getOrDefault(consumerGroupTimestamp, 0L) + minLag);
            }
        }

        for (HashMap<String, String> consumerGroupTimestamp2 : consumedMap.keySet()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("consumerGroup", consumerGroupTimestamp2.keySet().toArray()[0]);
            jsonObject.put("timestamp", consumerGroupTimestamp2.values().toArray()[0]);
            jsonObject.put("consumed", consumedMap.getOrDefault(consumerGroupTimestamp2, 0L));
            jsonObject.put("lag", minLagMap.getOrDefault(consumerGroupTimestamp2, 0L));
            output.add(jsonObject);
        }

        return new ResponseEntity<>(output.toString(), HttpStatus.OK);
    }

    static ResponseEntity<String> getConsumerInfo(KafkaStreams streams, ConsumerInfoModel consumerInfoModel) throws InterruptedException {
        String storeName = "offset_lag";

        waitUntilStoreIsQueryable(storeName, QueryableStoreTypes.windowStore(), streams);
        ReadOnlyWindowStore<String, Double> windowStore = streams.store(storeName, QueryableStoreTypes.windowStore());

        return getConsumerInfoFromStore(windowStore, consumerInfoModel);
    }

    static ResponseEntity<String> getConsumerInfoFromStore(ReadOnlyWindowStore<String, Double> windowStore, ConsumerInfoModel consumerInfoModel) {


//
        Instant timeFrom = Instant.ofEpochMilli(Long.parseLong(consumerInfoModel.getStartTime()));
        Instant timeTo = Instant.ofEpochMilli(Long.parseLong(consumerInfoModel.getEndTime()));
        KeyValueIterator<Windowed<String>, Double> iterator = windowStore.fetchAll(timeFrom, timeTo);
        ArrayList<JSONObject> output = new ArrayList<>();

        HashMap<HashMap<String, String>, Long> consumedMap = new HashMap<>();
        HashMap<HashMap<String, String>, Long> minLagMap = new HashMap<>();


        while (iterator.hasNext()) {

            KeyValue<Windowed<String>, Double> next = iterator.next();
            Windowed<String> windowTimestamp = next.key;
            if (windowTimestamp.key().startsWith(consumerInfoModel.getTopicName()) && windowTimestamp.key().split(":")[windowTimestamp.key().split(":").length - 1].equals(consumerInfoModel.getConsumerName())) {
                HashMap<String, String> consumerGroupTimestamp = new HashMap<>();


                String consumerGroup = windowTimestamp.key().split(":")[windowTimestamp.key().split(":").length - 1];

                String startTime = windowTimestamp.window().startTime().toString();

                consumerGroupTimestamp.put(consumerGroup, startTime);
                Long consumed = Long.parseLong(String.valueOf(next.value).split(":")[0]);
                Long minLag = Long.parseLong(String.valueOf(next.value).split(":")[1]);

                if (consumedMap.containsKey(consumerGroupTimestamp)) {
                    consumedMap.replace(consumerGroupTimestamp, consumedMap.getOrDefault(consumerGroupTimestamp, 0L) + consumed);
                }
                else {
                    consumedMap.put(consumerGroupTimestamp, 0L + consumed);
                }

                if (minLagMap.containsKey(consumerGroupTimestamp)) {
                    minLagMap.replace(consumerGroupTimestamp, minLagMap.getOrDefault(consumerGroupTimestamp, 0L) + minLag);
                }
                else {
                    minLagMap.put(consumerGroupTimestamp, 0L + minLag);
                }
//                consumedMap.put(consumerGroupTimestamp, consumedMap.getOrDefault(consumerGroupTimestamp, 0L) + consumed);
//                minLagMap.put(consumerGroupTimestamp, minLagMap.getOrDefault(consumerGroupTimestamp, 0L) + minLag);
            }
        }

        for (HashMap<String, String> consumerGroupTimestamp2 : consumedMap.keySet()) {
            JSONObject j1=new JSONObject();
            j1.put("consumer_group")
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("consumerGroup", consumerGroupTimestamp2.keySet().toArray()[0]);
            jsonObject.put("timestamp", consumerGroupTimestamp2.values().toArray()[0]);
            jsonObject.put("consumed", consumedMap.getOrDefault(consumerGroupTimestamp2, 0L));
            jsonObject.put("lag", minLagMap.getOrDefault(consumerGroupTimestamp2, 0L));
            output.add(jsonObject);
        }

        return new ResponseEntity<>(output.toString(), HttpStatus.OK);
    }

        public static <T> T waitUntilStoreIsQueryable(final String storeName, final QueryableStoreType<T> queryableStoreType, final KafkaStreams streams) throws InterruptedException {
            while (true) {
                try {
                    return streams.store(storeName, queryableStoreType);
                } catch (final InvalidStateStoreException ignored) {
                    // store not yet ready for querying
                    Thread.sleep(50);
                }
            }
        }
}
