import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import record.SongRecord
import utils.Utils

/**
  * @author Manthan Thakar
  */
object Graph {

  val SongInfoPath: String = "input/MillionSongSubset/song_info.csv.gz"
  val ArtistSimilarityPath: String = "input/MillionSongSubset/similar_artists.csv.gz"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Graph").setMaster("local")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val songData : RDD[SongRecord] = Utils.processCSV(sc.textFile(SongInfoPath))
      .map( x => new SongRecord(x.split(";"))).cache

    val artistSimilarity = Utils.processCSV(sc.textFile(ArtistSimilarityPath))
        .map(x => (x.split(";")(0), x.split(";")(1))).cache
    val artistSongs: RDD[(String, Int)] = songData.map(x => (x.artistId, 1)).reduceByKey(_ + _)
    val numSimilarArtists = artistSimilarity.map(x => (x._1, 1)).reduceByKey(_ + _)
    val artistFamiliarity = songData.map(x => (x.artistId, x.artistFamiliarity))
        .combineByKey(
          (v: Float) => (v, 1),
          (acc: (Float, Int), v: Float) => (acc._1 + v, acc._2 + 1),
          (acc1: (Float, Int), acc2:(Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
        .mapValues(x => x._1 / x._2.toFloat)

    val trendSetters = artistSongs
      .join(numSimilarArtists)
      .join(artistFamiliarity)
      .mapValues(x => x._1._1 * x._1._2 * x._2)
      .top(30)(Ordering[Float].on(_._2))

    trendSetters.foreach(println)
  }
}