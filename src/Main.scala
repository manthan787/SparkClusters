import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import record.SongRecord

/**
  * @author Manthan Thakar
  */
object Main {

  val SongInfoPath: String = "input/MillionSongSubset/song_info.csv.gz";

  def parseCSV(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex {
      case (0, iter) => iter.drop(1)
      case (_, iter) => iter
    }
  }

  def main(args: Array[String]): Unit = {
    // Get song data
    val sparkConf = new SparkConf().setAppName("Clustering")
    val sc = new SparkContext(sparkConf)
    val songData = parseCSV(sc.textFile(SongInfoPath)).map( x => new SongRecord(x.split(";")))

  }
}