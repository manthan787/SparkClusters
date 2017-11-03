import clustering.AgglomerativeClustering
import jobs.KmeansJobs
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import record.SongRecord
import utils.Utils

/**
  * @author Manthan Thakar
  */
object Cluster {

  def main(args: Array[String]): Unit = {
    val SongInfoPath: String = args(0) + "song_info.csv.gz"
    val sparkConf = new SparkConf().setAppName("Clustering").setMaster("local")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val songData : RDD[SongRecord] = Utils.processCSV(sc.textFile(SongInfoPath))
                                          .map( x => new SongRecord(x.split(";"))).cache
    // Run all Kmeansjobs
    val j = new KmeansJobs()
    j.execute(sc, songData, args(1))

    // Run agglomerative clustering on loudness
    // It's too slow, so I'll spare you by running just one job.
    val a = new AgglomerativeClustering()
    a.execute(songData.map(_.loudness).zipWithUniqueId()
                                      .map(x => (x._2.toString, x._1)))
  }
}