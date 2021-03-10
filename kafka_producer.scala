package Kafka_Assessment

import javax.swing.plaf.basic.BasicBorders
import scala.util.Random

object kafka_producer extends  App {
  // comments
  import java.util.Properties // producer settings
  // _ means import all
  import org.apache.kafka.clients.producer._

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  // kafka only knows bytes
  // producer to convert key to bytes, values to bytes
  // producer will have serializer
  // the key which is string type to bytes
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "Kafka_Assessment.kafka_serializer")

  //[String, Invoice] means key is String type, Value  is Invoice type
  // value is converted to bytes by InvoiceSerializer
  val producer = new KafkaProducer[String, Orders](props)

  val TOPIC = "order1";
  val random: Random = new Random()
  val states = Seq("AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","MD","MA","MI","MN","MS","MO","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY")

  for (i <- 1 to 100000) {
    val order_id = i // Order1, Order2...
    val item_id = (1 + random.nextInt(100)).toString
    val price = 1 + random.nextInt(50)
    val qty = 1 + random.nextInt(10)
    val state = states(random.nextInt(states.length))
    val orders_full = Orders(order_id,item_id ,price ,qty ,state)
    val record = new ProducerRecord(TOPIC, order_id.toString, orders_full)

    println("Writing " + order_id + ":" + orders_full)
    // this will call serialize to serialize invoice into json bytes
    producer.send(record)

    Thread.sleep(5000)
  }

}
