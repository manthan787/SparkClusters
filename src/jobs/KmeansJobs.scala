package jobs

import clustering.{Kmeans, Kmeans2D}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import record.SongRecord

/**
  * @author Manthan Thakar
  *
  * This class calls all the Kmeans jobs
  */
class KmeansJobs {

  def execute(sc: SparkContext, songData: RDD[SongRecord], outputBase: String) : Unit = {
    val fuzzyLoudness = new Kmeans(sc, songData.map(_.loudness), 10)
    fuzzyLoudness.run(outputBase + "kmeans-loudness")

    val fuzzyLength = new Kmeans(sc, songData.map(_.duration), 10)
    fuzzyLength.run(outputBase + "kmeans-length")

    val fuzzyTempo = new Kmeans(sc, songData.map(_.tempo), 10)
    fuzzyTempo.run(outputBase + "kmeans-tempo")

    val fuzzyHotness = new Kmeans(sc, songData.map(_.songHotness), 10)
    fuzzyHotness.run(outputBase + "kmeans-hotness")

    val combinedHotness = new Kmeans2D(sc, songData.map(_.songHotness),
      songData.map(_.artistHotness), k = 3, maxIter = 10)
    combinedHotness.run(outputBase + "combined-hotness-expr")
  }
}