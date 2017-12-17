package movieRatings
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object loadingFiles {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  def main(args: Array[String]) {
    val masterSet = "local[*]"
    val conf = new SparkConf().setAppName("loadingFiles").setMaster(masterSet)
    val sc = new SparkContext(conf)

    val myRatings: Seq[Rating] = loadRatings("in/ml-latest-small/ratings.csv")
    val myRatingsRDD = sc.parallelize(myRatings, 1)

    myRatingsRDD.saveAsTextFile("out/movieRatings/loadingFiles-" + masterSet + "-" + System.currentTimeMillis)
  }
  /** Load ratings from file. */
  def loadRatings(path: String): Seq[Rating] = {
    val lines: Iterator[String] = Source.fromFile(path).getLines()
    val ratings = lines.drop(1).map { line =>
      val fields = line.split(",")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.filter(_.rating > 0.0)
    if (ratings.isEmpty) {
      sys.error("No ratings provided.")
    } else {
      ratings.toSeq
    }
  }
}
