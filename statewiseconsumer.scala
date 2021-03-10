package Kafka_Assessment
import java.util
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties


object statewiseconsumer extends App{

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "Kafka_Assessment.statewisedeserializer")
  props.put("group.id", "state_earning-consumer-group")

  //[String, Invoice] means key is String type, Value is Invoice  type
  val consumer = new KafkaConsumer[String, statewiseearning](props)

  val TOPIC = "statewiseearning"
  consumer.subscribe( util.Collections.singletonList(TOPIC))

  while (true) {
    // consumer poll data from broker
    // poll function internally calls deserialize function
    // that converts bytes to invoice
    val records = consumer.poll(500)

    for (record <- records.asScala) {
      val key = record.key()
      print(record.value())
      // object type, by deserializer
      val value: statewiseearning = record.value()
      println("Earning Obj " + value)


    }
  }
}
