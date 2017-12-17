package movieRatings
import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import common.Utils

import scala.io.Source

object loadingFiles {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  def main(args: Array[String]) {
    val masterSet = "local[*]"
    val conf = new SparkConf().setAppName("loadingFiles").setMaster(masterSet)
    val sc = new SparkContext(conf)

    val myRatings: Seq[Rating] = loadRatings("in/ml-latest-small/myRatings.csv")
    val myRatingsRDD = sc.parallelize(myRatings, 1)

    //myRatingsRDD.saveAsTextFile("out/movieRatings/loadingFiles-" + masterSet + "-" + System.currentTimeMillis)
    val ratingsRDD = sc.textFile("in/ml-latest-small/ratings.csv").filter(line => line.split(Utils.COMMA_DELIMITER, -1)(0) != "userId")
      .map { line =>
      val fields = line.split(Utils.COMMA_DELIMITER, -1)
      // format: (timestamp % 10, Rating(userId, movieId, rating))
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    val moviesRDD: RDD[(Int, String)] = sc.textFile("in/ml-latest-small/movies.csv").filter(line => line.split(Utils.COMMA_DELIMITER, -1)(0) != "movieId")
      .map { line =>
      val fields = line.split(Utils.COMMA_DELIMITER, -1)
      // format: (movieId, movieName)
      (fields(0).toInt, fields(1))
    }

    //get ratings of user on top 50 popular movies
    val mostRatedMovieIds = ratingsRDD.map(_._2.product) //extract movieId
      .countByValue      //count ratings per movie
      .toSeq             //convert map to seq
      .sortBy(- _._2)    //sort by rating count in decreasing order
      .take(50)          //take 50 most rated
      .map(_._1)         //get movie ids
    for ((movieId) <- mostRatedMovieIds) println( movieId)
  }

  /** Load ratings from file. */
  def loadRatings(path: String): Seq[Rating] = {
    val lines: Iterator[String] = Source.fromFile(path).getLines()
    val ratings = lines.drop(1).map { line =>
      val fields = line.split(Utils.COMMA_DELIMITER, -1)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.filter(_.rating > 0.0)
    if (ratings.isEmpty) {
      sys.error("No ratings provided.")
    } else {
      ratings.toSeq
    }
  }
}
