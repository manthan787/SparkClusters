package clustering

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import record.SongRecord

class Kmeans(sc: SparkContext, values: RDD[Double], maxIter:Int) {

  def run(outputPath: String = "output/kmeans") = {
    var centroids: RDD[(Int, Double)] = sc.parallelize(Array(values.max, values.min, values.mean)
      .zipWithIndex).map(_.swap)

    var i = 0
    val MAX_ITER = 10
    var clusters: RDD[(Int, Double)] = null
    while (i < MAX_ITER) {
      i += 1
      clusters = getClusters(centroids, values)
      centroids = getCentroids(clusters, centroids)
    }
    clusters.map(x => x._1 + "," +  x._2)
      .saveAsTextFile(outputPath)
  }

  /**
    * Given the centroids and data points, it assigns clusters to each data point
    * by calculating the distance to each centroid and choosing the cluster
    * represented by the closest centroid
    * @param centroids Centroids for quiet, medium and loud clusters
    * @param songData Song data records
    * @return each data point with cluster assigned
    */
  def getClusters(centroids: RDD[(Int, Double)], songData: RDD[Double]): RDD[(Int, Double)] = {
    val c : Array[(Int, Double)] = centroids.collect
    songData.map(x => (c.map(c => (c._1, math.abs(c._2 - x))).minBy(y => y._2)._1, x))
  }

  /**
    * Given assigned clusters and centroids, calculates new centroids by getting the mean for
    * each clusters
    * @param clusters
    * @param centroids
    * @return
    */
  def getCentroids(clusters: RDD[(Int, Double)], centroids: RDD[(Int, Double)]): RDD[(Int, Double)] = {
    clusters
      // We do a full outer join to include centroids that don't have any values in it
      .fullOuterJoin(centroids)
      // Combine by cluster ID and calculate means for the clusters
      .combineByKey[(Double, Int)](
      // Create combiner
      // checks whether there is a Song Record available,
      //  if so it emits the loudness with count 1
      // Otherwise it emits 0 with count 1
      (v: (Option[Double], Option[Double])) => (if (v._1.isEmpty) 0 else v._1.get, 1),

      // Merge Value
      // This function similarly merges loudness values that we've seen before in the same partition
      (acc: (Double, Int), v : (Option[Double], Option[Double]))
      => (acc._1 + (if (v._1.isEmpty) 0 else v._1.get), acc._2 + 1),

      // Merge Combiners
      // Merges values for loudness across partitions
      (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

      // Calculate Mean for the clusters
      .mapValues(x => x._1 / x._2.toFloat)
  }
}
