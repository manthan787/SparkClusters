import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import record.{ArtistTermRecord, SongRecord}
import utils.Utils

/**
  * @author Manthan Thakar
  *
  * Solution for subproblem 2
  */
object Graph {

  val SongInfoPath: String = "input/MillionSongSubset/song_info.csv.gz"
  val ArtistSimilarityPath: String = "input/MillionSongSubset/similar_artists.csv.gz"
  val ArtistTermPath: String = "input/MillionSongSubset/artist_terms.csv.gz"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Graph").setMaster("local")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val songData : RDD[SongRecord] = Utils.processCSV(sc.textFile(SongInfoPath))
      .map( x => new SongRecord(x.split(";"))).cache

    val artistSimilarity = Utils.processCSV(sc.textFile(ArtistSimilarityPath))
        .map(x => (x.split(";")(0), x.split(";")(1))).cache

    val artistTermsData = Utils.processCSV(sc.textFile(ArtistTermPath))
      .map(x => new ArtistTermRecord(x.split(";"))).cache

    val artistSongs: RDD[(String, Int)] = songData.map(x => (x.artistId, 1)).reduceByKey(_ + _)
    val numSimilarArtists = artistSimilarity.map(x => (x._1, 1)).reduceByKey(_ + _)
    val artistFamiliarity = songData.map(x => (x.artistId, x.artistFamiliarity))
        .combineByKey(
          (v: Float) => (v, 1),
          (acc: (Float, Int), v: Float) => (acc._1 + v, acc._2 + 1),
          (acc1: (Float, Int), acc2:(Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
        .mapValues(x => x._1 / x._2.toFloat)

    val trendSetters = sc.parallelize(artistSongs
      .join(numSimilarArtists)
      .join(artistFamiliarity)
      .mapValues(x => x._1._1 * x._1._2 * x._2)
      .top(30)(Ordering[Float].on(_._2)))

    val artistTerms: RDD[(String, List[String])] = artistTermsData
      .map(x => (x.artistId, x.artistTerm))
      .combineByKey[List[String]](
        (v:String) => List[String](v),
        (acc: List[String], v: String ) => v :: acc,
        (acc1: List[String], acc2: List[String]) => acc1 ::: acc2
      )
    var centroids = trendSetters.join(artistTerms).map(x => x._2._2).zipWithIndex.map(_.swap)
    centroids.map(x => x._1 + "," + x._2.distinct.mkString("\t")).saveAsTextFile("output/initcentroids1")
    var i = 0
    var clusters: RDD[(Long, List[String])] = null
    while ( i < 10) {
      println("Entering iteration :" + i)
      clusters = getClusters(centroids, artistTerms)
      centroids = getCentroids(clusters, centroids)
      i += 1
    }
    centroids.map(x => x._1 + "," + x._2.distinct.mkString("\t")).saveAsTextFile("output/endcentroids1")
  }

  def getClusters(centroids: RDD[(Long, List[String])], artistTerms: RDD[(String, List[String])]): RDD[(Long, List[String])] = {
    val c: Array[(Long, List[String])] = centroids.collect
    artistTerms.map(x => (c.map(y => (y._1, x._2.intersect(y._2).length)).maxBy(z => z._2)._1, x._2))
  }

  def getCentroids(clusters: RDD[(Long, List[String])], centroids: RDD[(Long, List[String])]): RDD[(Long, List[String])] = {
    clusters
      .fullOuterJoin(centroids)
        .combineByKey[List[String]](
          (v: (Option[List[String]], Option[List[String]])) => if (v._1.isEmpty) v._2.get else v._1.get,
          (acc: (List[String]), v: (Option[List[String]], Option[List[String]])) => v._2.get.intersect(acc),
          (acc1: List[String], acc2: List[String]) => acc1 ::: acc2)
  }
}