package clustering

import org.apache.spark.rdd.RDD

class AgglomerativeClustering {

  def execute(m: RDD[(String, Double)]) = {
    // Create the distance matrix containing distances between each pair
    var distances = m.cartesian(m)
      .filter(x => x._1._1 != x._2._1)
      .map(x => ((x._1._1, x._2._1), math.abs(x._1._2 - x._2._2))).cache
    var i = 0
    while (  i < 100 ) {
      println("Entering Iteration: " + i)
      // Find the minimum pair in the Distance Matrix
      val minPair = distances.min()(Ordering[Double].on(x => x._2))
      val clusterKey = minPair._1._1 + "-" + minPair._1._2
      println("Cluster Key " + clusterKey)

      val matUpdate = distances
                        .filter(x => (x._1._1 == minPair._1._1 || x._1._1 == minPair._1._2)
                          && x._1._2 != minPair._1._1 && x._1._2 != minPair._1._2)
                        .map(x => ((clusterKey, x._1._2), x._2))

      // Perform complete linkage
      var updatedDistances = matUpdate.reduceByKey(Array(_, _).min)
      updatedDistances = updatedDistances.union(updatedDistances.map(x => ((x._1._2, x._1._1), x._2)))

      // Add the merged cluster in the distance matrix and remove the individual entries
      distances = distances
        .filter(x => x._1._1 != minPair._1._1 && x._1._2 != minPair._1._1
          && x._1._1 != minPair._1._2 && x._1._2 != minPair._1._2)
        .union(updatedDistances).cache
      i += 1
    }
    distances.map(x => x._1._1).distinct.saveAsTextFile("output/agglomerative")
  }
}
