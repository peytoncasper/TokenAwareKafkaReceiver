import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.security.KeyStore
import javax.net.ssl._

import com.datastax.driver.core.TokenRange
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.kafka.common.TopicPartition
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import collection.JavaConverters._

import scala.collection.mutable

class TokenAwareSocketReceiver(host: String, port: Int, id: String, cassandraConnector: CassandraConnector)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  var partitions: (List[String], mutable.HashMap[TopicPartition, String]) = null

  def mapPartitionsToCassandra(cassandraConnector: CassandraConnector, keyspace: String): (List[String], mutable.HashMap[TopicPartition, String]) ={
    val clusterInfo = cassandraConnector.withSessionDo { session =>
      session.getCluster
    }
    val partitionMapToHost = new mutable.HashMap[TopicPartition, String]()
    val tokenRanges = clusterInfo.getMetadata.getTokenRanges().asScala
    var i = 0
    var partitions = List[String]()
    tokenRanges.foreach((range: TokenRange) => {
      val replicas = clusterInfo.getMetadata.getReplicas(keyspace, range).toArray()
      partitionMapToHost.put(new TopicPartition(range.getStart + "_" + range.getEnd, i), replicas(0).toString)
      i += 1
      partitions = partitions ++ List(range.getStart + "_" + range.getEnd)
    })
    (partitions, partitionMapToHost)
  }

  def onStart() {
    partitions = mapPartitionsToCassandra(cassandraConnector, "ups")
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  override def preferredLocation(): Option[String] = {
    var location: Option[String] = null;

    if(id == 0)
      location = scala.Option.apply("54.202.107.200")
    else if(id == 1)
      location = scala.Option.apply("54.202.107.201")
    else if(id == 2)
      location = scala.Option.apply("54.218.94.197")


    return location
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {




    var socket: Socket = null
    var userInput: String = null

    try {

      socket = new Socket(host, port)


      // Until stopped or connection broken continue reading
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
      userInput = reader.readLine()
      while(!isStopped && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}
