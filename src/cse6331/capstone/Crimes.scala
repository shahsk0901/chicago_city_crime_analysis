package cse6331.capstone

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vectors
import java.security.MessageDigest

object Crimes {
	def main(args: Array[String]) {
	  
	  def knuthHash(word: String, constant: Int): Int = {
	    var hash = 0
	    for (ch <- word.toCharArray) {
	      hash = ((hash << 5) ^ (hash >> 27)) ^ ch.toInt
	    }
	     
	    hash % constant
  }
	    
	  // Defining spark context and configuration
	  val conf = new SparkConf().setAppName("Crimes").setMaster("local[2]")
		val sc = new SparkContext(conf)
		
		// Reading Input and filtering out any rows that has a null value
		var input = sc.textFile(args(0)).map(line=>{
		  var x = line.split(",") 
		  if(x.length == 4) {
		    var time = x(0).toString().split(" ")
		    var continuousTime = time(1).toString().split(":")
		    //var removeColonfromTime = continuousTime(0)+continuousTime(1)+continuousTime(2)
		    var finalTime = ""
		    if(time(2).toString() == "PM" && continuousTime(0).toInt != 12) {
		      finalTime = (continuousTime(0).toInt + 12).toString() + ":" + continuousTime(1) + ":" + continuousTime(2)
		    } else if(time(2).toString() != "PM" && continuousTime(0).toInt == 12) {
		      finalTime = (continuousTime(0).toInt - 12).toString() + ":" + continuousTime(1) + ":" + continuousTime(2)
		    } else {
		      finalTime = continuousTime(0) + ":" + continuousTime(1) + ":" + continuousTime(2)
		    }
		    
		    (finalTime,x(1).toString(),x(2).toInt,x(3).toInt)
		  } else {
		    (null,null,null.asInstanceOf[Int],null.asInstanceOf[Int])
		  }
		}).filter(x => x._1 != null)
		
		
		var transformInput = input.map(input => {
		  //var tim = MessageDigest.getInstance("MD5").digest(input._1.getBytes).map(0xFF & _ ).map {"%02x".format(_) }.foldLeft("") {_ + _}
		  //var tim = MessageDigest.getInstance("MD5").digest(input._1.getBytes).foldLeft("")(_+"%02x".format(‌​_))
		  var time = knuthHash(input._1, Int.MaxValue).toDouble
		  //var t = tim.toString().hashCode().toString()
		  //var time = tim.map(_.toByte.doubleValue()).reduceLeft((x,y) => x+y)
		  var crimeType = input._2.map(_.toByte.doubleValue()).reduceLeft((x,y) => x+y)
		  (time,crimeType,input._3,input._4)
		})
		
		
		val join = input.zip(transformInput)
		
		val vectors = transformInput.map(input => Vectors.dense(input._1,input._2,input._3.toDouble,input._4.toDouble))
		
		val clusters = 20
		val iterations = 20
		var p = KMeans.train(vectors, clusters, iterations)
		
		//join.foreach(println)
		println(clusters)
		p.clusterCenters.foreach(println)
		//println(p)
	
		sc.stop()
	}
}
