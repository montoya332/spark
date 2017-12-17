package movieRatings
import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
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

    //myRatingsRDD.saveAsTextFile("out/movieRatings/loadingFiles-" + masterSet + "-" + System.currentTimeMillis)
    val ratingsFile = sc.textFile(new File( "in/ml-latest-small/ratings.csv").toString)
    val ratings = ratingsFile.filter(r => r!=ratingsFile.first)
      .map { line =>
      val fields = line.split(",")
      // format: (timestamp % 10, Rating(userId, movieId, rating))
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    val moviesRDD: RDD[(Int, String)] = sc.textFile("in/ml-latest-small/movies.csv").filter(r => r!="movieId,title,genres").map { line =>
      val fields = line.split(",")
      // format: (movieId, movieName)
      (fields(0).toInt, fields(1))
    }
    val test = moviesRDD.collect()
   // moviesRDD.saveAsTextFile("out/movieRatings/loadingFiles-" + masterSet + "-" + System.currentTimeMillis)


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
