import java.nio.ByteBuffer

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.TopicPartition
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core._
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.writer.SqlRowWriter
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object TokenAwareConsumer {
  case class Test(test1: String, test2: String)
  def determineHost(cassandraConnector: CassandraConnector, keyspace: String, token: Token): Set[Host] ={
    var finalTokenRange: TokenRange = null
    val clusterInfo = cassandraConnector.withSessionDo { session =>
      session.getCluster
    }
    val tokenRanges: Set[TokenRange] = clusterInfo.getMetadata.getTokenRanges.asInstanceOf[Set[TokenRange]]


    tokenRanges.foreach(range => {
      if(range.contains(token))
        finalTokenRange = range
    })
    clusterInfo.getMetadata.getReplicas(keyspace, finalTokenRange).asInstanceOf[Set[Host]]

  }
  def mapPartitionsToCassandra(cassandraConnector: CassandraConnector, keyspace: String): (List[String], mutable.HashMap[TopicPartition, String]) ={
    val clusterInfo = cassandraConnector.withSessionDo { session =>
      session.getCluster
    }
    val partitionMapToHost = new mutable.HashMap[TopicPartition, String]()
    val tokenRanges = clusterInfo.getMetadata.getTokenRanges().asScala
    var i = 0
    var partitions = List[String]()
    // Map the host addresses to Kafka topics
    clusterInfo.getMetadata.getAllHosts.asScala.foreach((host: Host) => {

      val address = host.getAddress.getHostAddress.replaceAll("\\.","")
//      val replicas = clusterInfo.getMetadata.getReplicas(keyspace, range).toArray()
//      partitionMapToHost.put(new TopicPartition(range.getStart + "_" + range.getEnd, i), replicas(0).toString)
      partitionMapToHost.put(new TopicPartition(address, 0), host.getAddress.getHostAddress)
      i += 1
      partitions = partitions ++ List(address)
    })
    (partitions, partitionMapToHost)
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(30))
    val cassandraConnector = CassandraConnector(conf)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "54.149.233.128:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )



    val kafkaPartitions = mapPartitionsToCassandra(cassandraConnector, "ups")
    println(kafkaPartitions)

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferFixed(kafkaPartitions._2),
      ConsumerStrategies.Subscribe[String, String](kafkaPartitions._1, kafkaParams)
    )
//    val stream = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      PreferConsistent,
//      Subscribe[String, String](kafkaPartitions._1, kafkaParams)
//    )

    stream
      .map(record => (record.key, record.value))
      .mapPartitions(partition => {
        val map = new mutable.HashMap[String, String]
        partition.foreach(row => {
          if(row._1 != None) {
            val record = map.get(row._1).getOrElse(null)
            if (record == null)
              map.put(row._1, "1")
            else
              map.put(row._1, (record.toInt + 1).toString)
          }

        })
        map.iterator
      }, preservePartitioning = true)
      .mapPartitions(partition => {
        cassandraConnector.withSessionDo(session => {
          mergeRows(session, partition, 100)
        })
      })
//      .saveToCassandra("ups", "test", SomeColumns("test1", "test2"))
      .foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          cassandraConnector.withSessionDo(session => {
            saveRows(session, partition, 100)
          })
        })
      })
    ssc.start()
    ssc.awaitTermination()
  }
  def mergeRows(session: Session, partition: Iterator[(String, String)], rateLimit: Int): Iterator[Test] = {
    val preparedStatements = prepareStatements(session)
    var futures = new scala.collection.mutable.Queue[ResultSetFuture]()

    var mergedRows = ListBuffer[Test]()
    val t0 = System.currentTimeMillis
    var recordCount = 0
    partition.foreach(row => {
      val boundStatement = bindPreparedStatement(
        List(
          row._1
        ), preparedStatements("get"))



      if (futures.size >= rateLimit)
      {
        val future = futures.dequeue()
        future.getUninterruptibly()
      }

      val future = session.executeAsync(boundStatement)
      futures.enqueue(future)

      Futures.addCallback(
        future.asInstanceOf[ListenableFuture[ResultSet]],
        new FutureCallback[ResultSet]() {
          def onSuccess(resultSet: ResultSet): Unit = {
            val record = resultSet.one()
            if (record != null)
              mergedRows += Test(row._1, (row._2.toInt + record.getString("test2").toInt).toString)
            else
              mergedRows += Test(row._1, row._2)
          }

          def onFailure(thrown: Throwable) {
            print(thrown.getStackTrace.toString)
            print(thrown.getMessage.toString)
          }
        })
      recordCount += 1

    })
    while (futures.iterator.hasNext) {
      val future = futures.dequeue()
      future.getUninterruptibly()

    }
    println("Merge Elapsed time: " + (System.currentTimeMillis - t0) + "ms")
    println("Merged Records: " + recordCount)

    mergedRows.iterator

  }
  def saveRows(session: Session, partition: Iterator[Test], rateLimit: Int): Unit = {
    val preparedStatements = prepareStatements(session)
    var futures = new scala.collection.mutable.Queue[ResultSetFuture]()

    val t0 = System.currentTimeMillis
    var recordCount = 0
    partition.foreach(row => {

      val boundStatement = bindPreparedStatement(List(
        row.test1,
        row.test2
      ), preparedStatements("insert"))
//      val boundStatement = bindPreparedStatement(List(
//        row._1,
//        row._2
//      ), preparedStatements("insert"))

      if (futures.size >= rateLimit)
      {
        val future = futures.dequeue()
        future.getUninterruptibly()
      }


      val future = executeBoundStatement(session, boundStatement)
      futures.enqueue(future)

      recordCount += 1
    })

    while (futures.iterator.hasNext) {
      val future = futures.dequeue()
      future.getUninterruptibly()

    }
    println("Write Elapsed time: " + (System.currentTimeMillis - t0) + "ms")
    println("Write Records: " + recordCount)

  }
  def executeBoundStatement(session: Session, boundStatement: BoundStatement): ResultSetFuture = {

    val future = session.executeAsync(boundStatement)
    Futures.addCallback(future.asInstanceOf[ListenableFuture[ResultSet]],
      new FutureCallback[ResultSet]() {
        def onSuccess(resultSet: ResultSet): Unit = {
        }

        def onFailure(thrown: Throwable) {
          print(thrown.getStackTrace.toString)
          print(thrown.getMessage.toString)
        }
      })

    future


  }
  def bindPreparedStatement(variables: List[Any], statement: PreparedStatement): BoundStatement = {
    val boundStatement = statement.bind()
    for (i <- 0 to variables.length -1) {
      val variableType = variables(i).asInstanceOf[AnyRef]
      if(variableType.isInstanceOf[Int])
        boundStatement.setInt(i, variables(i).asInstanceOf[java.lang.Integer])
      else if(variableType.isInstanceOf[String])
        boundStatement.setString(i, variables(i).asInstanceOf[String])
      else if(variableType.isInstanceOf[java.util.Date])
        boundStatement.setTimestamp(i, variables(i).asInstanceOf[java.util.Date])
    }

    boundStatement
  }
  def prepareStatements(session: Session): Map[String, PreparedStatement] =
  {
    val get =
      session.prepare("SELECT test2 FROM ups.test WHERE test1=?")
    val insert =
      session.prepare(
        "INSERT INTO ups.test " +
          "(test1, test2) " +
          "VALUES (?,?)")
    Map(
      "get" -> get,
      "insert" -> insert
    )
  }
}
