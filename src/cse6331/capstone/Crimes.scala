package cse6331.capstone

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

object Crimes {
  
  // Hash function to convert string to hash code
	  def knuthHash(word: String, constant: Int): Int = {
	    var hash = 0
	    for (ch <- word.toCharArray) {
	      hash = ((hash << 5) ^ (hash >> 27)) ^ ch.toInt
	    }
	     
	    hash % constant  
	  }
  
  // Calculating squared distance between a record and a cluster centroid
  def calculateDistance(record: (Double,Double,Double,Double),centroid: ((Double,Double,Double,Double))) = {
    (Math.pow(record._1 - centroid._1, 2) + 
    Math.pow(record._2 - centroid._2, 2) + 
    Math.pow(record._3 - centroid._3, 2) +
    Math.pow(record._4 - centroid._4, 2))
  }

  // Adding records for each cluster to measure weight of each cluster
  def add(record1: (Double,Double,Double,Double),record2: ((Double,Double,Double,Double))) = {
	    (record1._1 + record2._1,
	    record1._2 + record2._2,
	    record1._3 + record2._3,
	    record1._4 + record2._4)
	  }
	  
	  // Finding the cluster centroid to which the record is closest to and returning the cluster id
	  def closestToCluster(record: (Double, Double, Double, Double), clusters: Array[(Double, Double, Double, Double)]): Int = {
			  
	    var clusterAssigned = 0	  
			var minimumDistance = Double.PositiveInfinity

		  for (i <- 1 to clusters.length) {
			  val distanceToCentroid = calculateDistance(record, clusters(i-1))
					  if (distanceToCentroid < minimumDistance) {
						  minimumDistance = distanceToCentroid
								  clusterAssigned = i
					  }
		  }
      clusterAssigned
	  }
	  
	  def runKmeans(transformInput: RDD[(Double,Double,Double,Double)],sc: SparkContext): 
	  (RDD[((Double,Double,Double,Double),Int)],RDD[(Int)]) = { 
	    
  	  // Initializing the RDD to assign the cluster to each point
  		var closestTo = transformInput.map(x => (0,(x,0)))
  		
  		// Randomly initialize cluster centroids
  		var clusters = transformInput.takeSample(false,10,System.nanoTime())
  		
  		
  	  var currentIteration = 1
  	  val convergence = 0.01
  	  var dist = Double.PositiveInfinity
  		
  	  while ( dist >  convergence) {
  		  
  		  closestTo = transformInput.map(record => { 
  		    var clusterAssigned = closestToCluster(record, clusters)
  		    (clusterAssigned, (record,1))
  		    
  		  })
  		 
  		  val calculateClusters = closestTo.reduceByKey {
  		    case ((record1,r1), (record2,r2)) => (add(record1,record2),r1+r2) 
  		  }
  		  
  		  var calculateNewClusterCentroid = calculateClusters.map { 
  		    case(clusterIndex, (clusterInfo,totalRecords)) => (clusterIndex, (clusterInfo._1/totalRecords,
  		        clusterInfo._2/totalRecords,
  		        clusterInfo._3/totalRecords,
  		        clusterInfo._4/totalRecords))
  		  }.collectAsMap()
  		  
  		  
  		  /*println("Iteration number: " + currentIteration + "\n")
  		  calculateNewClusterCentroid.foreach(println)
  		  
  		  
  		  println("\n\n\n\nIteration " + currentIteration + ": ")
  		  
  		  println("\n\nInitials Clusters: ")
  		  clusters.foreach(println)
  		  
  		  println("\n\nCalculated: ")
  		  calculateNewClusterCentroid.foreach(println)*/
  		  
  		  dist = 0.0
  		  for (i <- 1 to 10) {
          dist += calculateDistance(clusters(i-1), calculateNewClusterCentroid(i))
        }
  		  
  		  
  		  // Assign new cluster centroids
  		  for(i <- 1 to clusters.length) {
  		    clusters(i-1) = calculateNewClusterCentroid(i)
  		  }
  		  
  		  //println("\n\nNew Clusters: ")
  		  //clusters.foreach(println)
  
  		  currentIteration+=1
  	  }
  		var i = 0
  		val returnClusters = sc.parallelize(clusters.map(f=> {
  		  i+=1
  		  ((f._1,f._2,f._3,f._4),i)
  		}))
  		
  		(returnClusters,closestTo.map(f=>f._1))
	  }


	def main(args: Array[String]) {	  
	  
	  // Defining spark context and configuration
	  val conf = new SparkConf().setAppName("Crimes").setMaster("local[1]")
		val sc = new SparkContext(conf)
		
		// Reading Input and filtering out any rows that has a null value
		var input = sc.textFile(args(0)).map(line=>{
		  var x = line.split(",") 
		  if(x.length == 4) {
		    
		    // Cleaning time dimension to proper value
		    var time = x(0).toString().split(" ") 
		    var continuousTime = time(1).toString().split(":")
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
		  var time = knuthHash(input._1, Int.MaxValue).toDouble
		  var crimeType = input._2.map(_.toByte.doubleValue()).reduceLeft((x,y) => x+y)
		  var district = input._3.toDouble
		  var community = input._4.toDouble
		  (time,crimeType,district,community)
		})
		
		var returnValue = runKmeans(transformInput, sc)
		
		val clusters = returnValue._1
		val recordsAssignedToCluster = returnValue._2
	
		val temp = input.zip(transformInput)
		val result = temp.zip(recordsAssignedToCluster)
		
		result.foreach(println)
		clusters.foreach(println)
	  
				
		sc.stop()
	}
}