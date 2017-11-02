package jobs

import clustering.Kmeans
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import record.SongRecord

/**
  * @author Manthan Thakar
  *
  * This class calls all the Kmeans jobs
  */
class KmeansJobs {

  def execute(sc: SparkContext, songData: RDD[SongRecord]) : Unit = {
    val fuzzyLoudness = new Kmeans(sc, songData.map(_.loudness), 10)
    fuzzyLoudness.run("output/kmeans-loudness")

    val fuzzyLength = new Kmeans(sc, songData.map(_.duration), 10)
    fuzzyLength.run("output/kmeans-length")

    val fuzzyTempo = new Kmeans(sc, songData.map(_.tempo), 10)
    fuzzyTempo.run("output/kmeans-tempo")

    val fuzzyHotness = new Kmeans(sc, songData.map(_.songHotness), 10)
    fuzzyHotness.run("output/kmeans-hotness")
  }
}