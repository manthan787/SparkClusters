package utils

import org.apache.spark.rdd.RDD

/**
  * @author Manthan Thakar
  * This singleton contains utility methods required by multiple jobs
  */
object Utils {

  /**
    * Given an RDD[String] it drops the first row (header)
    * @param data
    * @return RDD[String] with first row dropped
    */
  def processCSV(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex {
      case (0, iter) => iter.drop(1)
      case (_, iter) => iter
    }
  }
}
