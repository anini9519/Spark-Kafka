package Kafka_Assessment

import org.apache.kafka.common.serialization.Deserializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

class statewisedeserializer extends  Deserializer[statewiseearning]{
  val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)
  // convert bytes to  Invoice object
  // when consumer calls poll method, this method is invoked automatically

  override def deserialize(topic: String, bytes: Array[Byte]): statewiseearning = {

    val earning: statewiseearning = objectMapper.readValue[statewiseearning](bytes)


    //      println("earning "+earning)
    earning // return the object to consumer
  }
}