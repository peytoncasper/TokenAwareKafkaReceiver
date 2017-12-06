import org.apache.spark.Partitioner

class TokenAwarePartitioner (partitions: Int) extends Partitioner {

  override def numPartitions: Int = {
    partitions
  }

  def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[String]
      // `k` is assumed to go continuously from 0 to elements-1.
      return k.toInt - 1

  }
}
