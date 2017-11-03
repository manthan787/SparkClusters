import jobs.KmeansJobs
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import record.SongRecord
import utils.Utils

/**
  * @author Manthan Thakar
  */
object Main {

  val SongInfoPath: String = "input/MillionSongSubset/song_info.csv.gz"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Clustering").setMaster("local")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val songData : RDD[SongRecord] = Utils.processCSV(sc.textFile(args(0)))
                                          .map( x => new SongRecord(x.split(";"))).cache

    val j = new KmeansJobs()
    j.execute(sc, songData, args(1))
//    val m: RDD[(String, Double)] = songData.map(_.loudness).zipWithUniqueId()
//                                  .map(x => (x._2.toString, x._1))
//    var distances = m.cartesian(m)
//      .filter(x => x._1._1 != x._2._1)
//      .map(x => ((x._1._1, x._2._1), math.abs(x._1._2 - x._2._2))).cache
//    var i = 0
//    while (  i < 100 ) {
//      System.out.println("Entering Iteration: " + i)
//      val minPair = distances.min()(Ordering[Double].on(x => x._2))
//      val clusterKey = minPair._1._1 + "-" + minPair._1._2
//      System.out.println("Cluster Key " + clusterKey)
//      val matUpdate = distances
//                        .filter(x => (x._1._1 == minPair._1._1 || x._1._1 == minPair._1._2)
//                          && x._1._2 != minPair._1._1 && x._1._2 != minPair._1._2)
//                        .map(x => ((clusterKey, x._1._2), x._2))
//      var updatedDistances = matUpdate.reduceByKey(Array(_, _).min)
//      updatedDistances = updatedDistances.union(updatedDistances.map(x => ((x._1._2, x._1._1), x._2)))
//      distances = distances
//        .filter(x => x._1._1 != minPair._1._1 && x._1._2 != minPair._1._1
//          && x._1._1 != minPair._1._2 && x._1._2 != minPair._1._2)
//        .union(updatedDistances).cache
//      i += 1
//    }
//    distances.map(x => x._1._1).distinct.saveAsTextFile(args(1))
  }
}