package cse6331.capstone

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vectors

object Crimes {
	def main(args: Array[String]) {
		
	  // Defining spark context and configuration
	  val conf = new SparkConf().setAppName("Crimes").setMaster("local[2]")
		val sc = new SparkContext(conf)
		
		// Reading Input and filtering out any rows that has a null value
		var input = sc.textFile(args(0)).map(line=>{
		  var x = line.split(",") 
		  if(x.length == 4) {
		    var time = x(0).toString().split(" ")
		    ((time(1) + "" + time(2)),x(1).toString(),x(2).toInt,x(3).toInt)
		  } else {
		    (null,null,null.asInstanceOf[Int],null.asInstanceOf[Int])
		  }
		}).filter(x => x._1 != null)
		
		
		var transformInput = input.map(input => {
		  var time = input._1.map(_.toByte.doubleValue()).reduceLeft((x,y) => x+y)
		  var crimeType = input._2.map(_.toByte.doubleValue()).reduceLeft((x,y) => x+y)
		  (time,crimeType,input._3,input._4)
		})
		
		
		val vectors = transformInput.map(input => Vectors.dense(input._1,input._2,input._3.toDouble,input._4.toDouble))
		
		val clusters = 10
		val iterations = 20
		var p = KMeans.train(vectors, clusters, iterations)
		
		p.clusterCenters.foreach(println)
	
		var prediction = p.predict(vectors)
		prediction.foreach(println)
		
		sc.stop()
	}
}