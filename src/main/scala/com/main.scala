package com

import com.kafka.KafkaClient
import scala.{Stream => _}
import fs2.{Stream, Task, Strategy}
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}

object main {

  implicit val stringDeserializer: Deserializer[String] = new StringDeserializer

  def main(args: Array[String]): Unit = {

    implicit val S = Strategy.fromFixedDaemonPool(2, threadName = "worker")

    val client: Task[KafkaClient[Task]] = KafkaClient.apply[Task]("localhost:9092")

    client.unsafeAttemptRun() match {
      case Right(c) =>
        val stream: Stream[Task, (String, String)] = c.subscribe[String, String]("test", 100, "foobar", 100, 10000, 100, 100, 100, OffsetResetStrategy.LATEST)
        println("results:" + stream.runLog.unsafeRun() )
      case Left(t)  => println(t.getMessage); sys.exit(-1)
    }
  }
}
