package com

import java.util.Properties
import java.util.concurrent.Executors

import fs2.{Chunk, Sink, Strategy, Stream}
import fs2.util.Async
import fs2.util.syntax._
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetResetStrategy, KafkaConsumer => Consumer}
import org.apache.kafka.clients.producer.{
  Callback,
  ProducerConfig,
  ProducerRecord,
  RecordMetadata,
  KafkaProducer => Producer
}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

// This entire file was written by https://gist.github.com/tscholak/f922c9f80eafe3253936cfc22d1f914c
object kafka {

  sealed trait KafkaClient[F[_]] {
    def publish[K, V](topic: String, acks: String, retries: Int, batchSize: Int, lingerMs: Long, bufferMemory: Int)(
      implicit keySerializer: Serializer[K],
      valueSerializer: Serializer[V]): Sink[F, (K, V)]
    def subscribe[K, V](topic: String,
                        pollTimeoutMs: Long,
                        groupId: String,
                        autoCommitIntervalMs: Long,
                        sessionTimeoutMs: Long,
                        maxPartitionFetchBytes: Int,
                        maxPollRecords: Int,
                        maxPollInterval: Int,
                        autoOffsetReset: OffsetResetStrategy)(implicit keyDeserializer: Deserializer[K],
                                                              valueDeserializer: Deserializer[V]): Stream[F, (K, V)]
  }

  object KafkaClient {
    def apply[F[_]](bootstrapServers: String)(implicit F: Async[F]): F[KafkaClient[F]] = {
      sealed trait KafkaPublisher[K, V] {
        def publish(topic: String): Sink[F, (K, V)]
        def teardown: F[Unit]
      }

      def mkKafkaPublisher[K, V](acks: String, retries: Int, batchSize: Int, lingerMs: Long, bufferMemory: Int)(
        implicit keySerializer: Serializer[K],
        valueSerializer: Serializer[V]): F[KafkaPublisher[K, V]] =
        F.delay[KafkaPublisher[K, V]] {
          val props: Properties = new Properties()
          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
          props.put(ProducerConfig.ACKS_CONFIG, acks)
          props.put(ProducerConfig.RETRIES_CONFIG, retries.toString)
          props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize.toString)
          props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs.toString)
          props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory.toString)
          val producer = new Producer[K, V](props, keySerializer, valueSerializer)

          new KafkaPublisher[K, V] {
            def publish(topic: String): Sink[F, (K, V)] =
              _ flatMap {
                case (key, value) =>
                  Stream.eval[F, RecordMetadata] {
                    F.async[RecordMetadata] { cb =>
                      F.delay {
                        producer.send(new ProducerRecord(topic, key, value), new Callback {
                          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
                            if (exception == null)
                              cb(Right(metadata))
                            else
                              cb(Left(exception))
                          }
                        })
                      } >> F.pure(())
                    }
                  } map { _ =>
                    ()
                  }
              }

            def teardown: F[Unit] = F.delay(producer.close())
          }
        }

      sealed trait KafkaSubscriber[K, V] {
        def subscribe(topic: String, pollTimeoutMs: Long): Stream[F, (K, V)]
        def teardown: F[Unit]
      }

      def mkKafkaSubscriber[K, V](groupId: String,
                                  autoCommitIntervalMs: Long,
                                  sessionTimeoutMs: Long,
                                  maxPartitionFetchBytes: Int,
                                  maxPollRecords: Int,
                                  maxPollInterval: Int,
                                  autoOffsetReset: OffsetResetStrategy)(
                                   implicit keyDeserializer: Deserializer[K],
                                   valueDeserializer: Deserializer[V]): F[KafkaSubscriber[K, V]] =
        F.delay[KafkaSubscriber[K, V]] {
          val props: Properties = new Properties()
          props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
          props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
          props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true.toString)
          props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMs.toString)
          props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs.toString)
          props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes.toString)
          props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
          props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval.toString)
          props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset.toString.toLowerCase)
          val consumer = new Consumer[K, V](props, keyDeserializer, valueDeserializer)

          new KafkaSubscriber[K, V] {
            def subscribe(topic: String, pollTimeoutMs: Long): Stream[F, (K, V)] = {
              Stream.bracket(F.delay(Executors.newSingleThreadExecutor))(
                executor => {
                  import collection.JavaConverters._
                  implicit val strategy = Strategy.fromExecutor(executor)
                  Stream.eval(F.delay(consumer.subscribe(List(topic).asJavaCollection))).flatMap { _ =>
                    Stream.repeatEval[F, List[(K, V)]](
                      F.delay(consumer.poll(pollTimeoutMs).records(topic).iterator.asScala.toList map { r =>
                        (r.key, r.value)
                      }))
                  } collect {
                    case scala.::(hd, tl) => Chunk.concat(List(Chunk.singleton(hd), Chunk.seq(tl)))
                  } flatMap {
                    Stream.chunk
                  }
                },
                executor => F.delay(executor.shutdown())
              )
            }

            def teardown: F[Unit] =
              F.delay {
                consumer.unsubscribe()
                consumer.close()
              }
          }
        }

      F.delay[KafkaClient[F]] {
        new KafkaClient[F] {
          def publish[K, V](topic: String,
                            acks: String,
                            retries: Int,
                            batchSize: Int,
                            lingerMs: Long,
                            bufferMemory: Int)(implicit keySerializer: Serializer[K],
                                               valueSerializer: Serializer[V]): Sink[F, (K, V)] =
            s =>
              Stream.bracket[F, KafkaPublisher[K, V], Unit](
                mkKafkaPublisher[K, V](acks, retries, batchSize, lingerMs, bufferMemory))(
                _.publish(topic)(s),
                _.teardown)

          def subscribe[K, V](topic: String,
                              pollTimeoutMs: Long,
                              groupId: String,
                              autoCommitIntervalMs: Long,
                              sessionTimeoutMs: Long,
                              maxPartitionFetchBytes: Int,
                              maxPollRecords: Int,
                              maxPollInterval: Int,
                              autoOffsetReset: OffsetResetStrategy)(
                               implicit keyDeserializer: Deserializer[K],
                               valueDeserializer: Deserializer[V]): Stream[F, (K, V)] =
            Stream.bracket[F, KafkaSubscriber[K, V], (K, V)](
              mkKafkaSubscriber[K, V](
                groupId,
                autoCommitIntervalMs,
                sessionTimeoutMs,
                maxPartitionFetchBytes,
                maxPollRecords,
                maxPollInterval,
                autoOffsetReset))(_.subscribe(topic, pollTimeoutMs), _.teardown)
        }
      }
    }
  }
}