import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.kstream.{Consumed, KStream, Materialized, TimeWindows}
import org.apache.kafka.streams.state.{QueryableStoreType, QueryableStoreTypes, ReadOnlyWindowStore, WindowStore}
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters, StreamsBuilder, StreamsConfig}
import org.json.JSONObject
import org.springframework.http.{HttpStatus, ResponseEntity}

import java.time.{Duration, Instant}
import java.util
import java.util.Properties


object OffsetAndLagApp {

  def start:KafkaStreams =
  {
    val schemaRegistryURL=sys.env.getOrElse[String]("SCHEMA_REGISTRY_URL",Application.SCHEMA_REGISTRY_URL)
    val bootstrapServers=sys.env.getOrElse("BOOTSTRAP_SERVERS",Application.BOOTSTRAP_SERVER)
    val OFFSET_CONSUMER_GROUP_ID = sys.env.getOrElse("OFFSET_CONSUMER_GROUP_ID", "consumer-offset3")

    val prop=new Properties();
    prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers)
    prop.put(StreamsConfig.APPLICATION_ID_CONFIG,"OffsetAndLag")
    prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,sys.env.getOrElse("AUTO_OFFSET_RESET_CONFIG","earliest"))
    prop.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG,"false")
    prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass)
    prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,GenericAvroSerde)
    prop.put(StreamsConfig.STATE_DIR_CONFIG, System.getenv.getOrDefault("STATE_STORE_LOCATION", "/tmp/kafka-streams"))
    prop.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL)
    prop.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor)

    val input_topic="topic_i1"
    val builder= new StreamsBuilder()
    // generating the Generic avro serde
    val valueGenericAvroSerde=new GenericAvroSerde
    // we need to configure the above serde , whether it is for key or for value
    val serde=new util.HashMap[String,String]
    serde.put("schema.registry.url",schemaRegistryURL)
    valueGenericAvroSerde.configure(serde,false)
    val keyGenericAvroSerde= new GenericPrimitiveAvroSerDe[GenericRecord]
    keyGenericAvroSerde.configure(serde,true)


    val stream1=builder.stream(input_topic,Consumed.`with`(keyGenericAvroSerde,valueGenericAvroSerde))
    // implementing the topology
    modifyStream(stream1,OFFSET_CONSUMER_GROUP_ID)
    // creating the kafkastream object
    val kafkastream1=new KafkaStreams(builder.build,prop)
    kafkastream1.start()
    kafkastream1
  }


  def modifyStream(stream1: KStream[GenericRecord, GenericRecord],consumerOffset:String): Unit =
  {
      val stream2=stream1
        .filter((k,v) => !(v.get("topic") == sys.env.getOrElse("CONSUMER_OFFSETS_OUTPUT_TOPIC", "consumerOffsetsTest3")) || !(v.get("topic") == consumerOffset+ "-offset_lag-repartition"))
        .groupBy((k,v) => v.get("topic").toString + ":" + v.get("partition").toString + ":" + v.get("consumerGroup").toString)
        .windowedBy(TimeWindows.of(Duration.ofHours(1)))
        .aggregate[OffsetAndLag](
          () => new OffsetAndLag(0L,Long.MaxValue),
          (k,v,a) => new OffsetAndLag( v.get("offset").toString.toLong max a.getOffset, 0 min a.getLag) ,
          Materialized.as[String, OffsetAndLag, WindowStore[Bytes, Array[Byte]]]("offset_lag").withKeySerde(Serdes.String).withValueSerde(new OffsetAndLagSerde)
        )
  }



  @throws(classOf[InterruptedException])
  def getConsumersInfo(k_stream_obj:KafkaStreams, consumersInfoModel:ConsumerInfoModel):ResponseEntity[String]=
  {
    val storeName = "offset_lag"

    waitUntilStoreIsQueryable(storeName, QueryableStoreTypes.windowStore, k_stream_obj)

    val windowStore:ReadOnlyWindowStore[String,Double] = k_stream_obj.store(storeName,QueryableStoreTypes.windowStore)
    //
    val timeFrom = Instant.ofEpochMilli(consumersInfoModel.getStartTime.toLong)
    val timeTo = Instant.ofEpochMilli(consumersInfoModel.getEndTime.toLong)
    val iterator = windowStore.fetchAll(timeFrom, timeTo)
    val output = List.empty[JSONObject]
//    val map1=Map((Map(" "->" "))->0L)
//    val map2=Map[String,String]
    var consumedMap = Map.empty[Map[String,String],Long]
    var minLagMap = Map.empty[Map[String,String],Long]

    while (iterator.hasNext) {
      val next = iterator.next
      val windowTimestamp = next.key
      if (windowTimestamp.key().startsWith(consumersInfoModel.getTopicName)) {
        val consumerGroupTimestamp = Map.empty[String,String]
        val consumerGroup = windowTimestamp.key.split(":")(windowTimestamp.key.split(":").length - 1)
        val startTime = windowTimestamp.window.startTime.toString
        consumerGroupTimestamp += (consumerGroup,startTime)
        //consumerGroupTimestamp.put(consumerGroup, startTime)
        val consumed = (String.valueOf(next.value).split(":")(0)).toLong
        val minLag = (String.valueOf(next.value).split(":")(1)).toLong

        if (consumedMap.contains(consumerGroupTimestamp)) consumedMap += consumerGroupTimestamp ->( consumedMap.getOrElse(consumerGroupTimestamp, 0L) + consumed )
        else consumedMap += consumerGroupTimestamp ->( 0L + consumed )
        if (minLagMap.contains(consumerGroupTimestamp)) minLagMap += consumerGroupTimestamp -> (minLagMap.getOrElse(consumerGroupTimestamp, 0L) + minLag)
        else minLagMap += consumerGroupTimestamp -> ( 0L + minLag )
        //                consumedMap.put(consumerGroupTimestamp, consumedMap.getOrDefault(consumerGroupTimestamp, 0L) + consumed);
        //                minLagMap.put(consumerGroupTimestamp, minLagMap.getOrDefault(consumerGroupTimestamp, 0L) + minLag);
      }
    }


    for (consumerGroupTimestamp2 <- consumedMap.keySet) {
      val jsonObject = new JSONObject
      val arr1=Array(consumerGroupTimestamp2.keySet)
      val arr2=Array(consumerGroupTimestamp2.values)
      jsonObject.put("consumerGroup", arr1(0))
      jsonObject.put("timestamp", arr2(0))
      jsonObject.put("consumed", consumedMap.getOrElse(consumerGroupTimestamp2, 0L))
      jsonObject.put("lag", minLagMap.getOrElse(consumerGroupTimestamp2, 0L))
      output.appended(jsonObject)
    }

    return new ResponseEntity[String](output.toString, HttpStatus.OK)
  }



  @throws(classOf[InterruptedException])
  def getConsumerInfo(streams: KafkaStreams, model: ConsumerInfoModel):  ResponseEntity[String] =
    {
      val storeName = "offset_lag"
      waitUntilStoreIsQueryable(storeName, QueryableStoreTypes.windowStore, streams)
      val windowStore:ReadOnlyWindowStore[String,Double]= streams.store(storeName, QueryableStoreTypes.windowStore)
      getConsumerInfoFromStore(windowStore, model)
    }

  @throws(classOf[InterruptedException])
  def getConsumerInfoFromStore(windowStore: ReadOnlyWindowStore[String, Double], consumerInfoModel: ConsumerInfoModel): ResponseEntity[String] =
    {
      val timeFrom = Instant.ofEpochMilli(consumerInfoModel.getStartTime.toLong)
      val timeTo = Instant.ofEpochMilli(consumerInfoModel.getEndTime.toLong)
      val iterator = windowStore.fetchAll(timeFrom, timeTo)
      val output = List.empty[JSONObject]


      var consumedMap = Map.empty[Map[String,String],Long]
      var minLagMap = Map.empty[Map[String,String],Long]

      while(iterator.hasNext)
      {
        val next= iterator.next
        val windowTimestamp = next.key
        if(windowTimestamp.key().startsWith(consumerInfoModel.getTopicName) && (windowTimestamp.key.split(":")(windowTimestamp.key.split(":").length - 1).equals(consumerInfoModel.getConsumerName)))
          {
            var consumerGroupTimestamp= Map.empty[String,String]
            val consumerGroup = windowTimestamp.key.split(":")(windowTimestamp.key.split(":").length - 1)
            val startTime = windowTimestamp.window.startTime.toString
            consumerGroupTimestamp += consumerGroup -> startTime
            val consumed = String.valueOf(next.value).split(":")(0).toLong
            val minLag = String.valueOf(next.value).split(":")(1).toLong

          //  ---------is there any difference between "contains" and "containskey"
            if (consumedMap.contains(consumerGroupTimestamp))
              consumedMap += consumerGroupTimestamp -> (consumedMap.getOrElse(consumerGroupTimestamp, 0L) + consumed)
            else
              consumedMap += consumerGroupTimestamp ->  (0L + consumed)

            if (minLagMap.contains(consumerGroupTimestamp))
              minLagMap += consumerGroupTimestamp -> (minLagMap.getOrElse(consumerGroupTimestamp, 0L) + minLag)
            else
              minLagMap += consumerGroupTimestamp -> (0L + minLag)
          }
      }

      for (consumerGroupTimestamp2 <- consumedMap.keySet) {
        val jsonObject = new JSONObject
        val arr1=Array(consumerGroupTimestamp2.keySet)
        val arr2=Array(consumerGroupTimestamp2.values.toArray)
        jsonObject.put("consumerGroup", arr1(0))
        jsonObject.put("timestamp", arr2(0))
        jsonObject.put("consumed", consumedMap.getOrElse(consumerGroupTimestamp2, 0L))
        jsonObject.put("lag", minLagMap.getOrElse(consumerGroupTimestamp2, 0L))
        output.appended(jsonObject)
      }

      new ResponseEntity[String](output.toString, HttpStatus.OK)

    }



 //-----------------here at try block return statement is not kept

  def waitUntilStoreIsQueryable(storeName: String, windowStore: QueryableStoreType[ReadOnlyWindowStore[String,Double]], streams: KafkaStreams)=
  {
    var i=true
    //val a=streams.store(StoreQueryParameters.fromNameAndType(storeName,windowStore))
    while ( i)
      try
      {
        i=false
        streams.store(StoreQueryParameters.fromNameAndType(storeName,windowStore))
      }
      catch
      {
        case ignored: InvalidStateStoreException => Thread.sleep(50)
      }
  }


}


class OffsetAndLag(offset:Long,lag:Long)
{
  def getOffset: Long =offset
  def getLag: Long =lag
  override def toString:String = offset.toString + " , " + lag.toString
}