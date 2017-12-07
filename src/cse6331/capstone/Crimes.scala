package cse6331.capstone

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors

object Crimes {
  
	def main(args: Array[String]) {
		
	  // Calculating squared distance between a record and a cluster centroid
	  def calculateDistance(record: (Double,Double,Double,Double),centroid: ((Double,Double,Double,Double))) = {
	    (Math.pow(record._1 - centroid._1, 2) + 
	    Math.pow(record._2 - centroid._2, 2) + 
	    Math.pow(record._3 - centroid._3, 2) +
	    Math.pow(record._4 - centroid._4, 2))
	  }
	  
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
	  
	  
	  // Defining spark context and configuration
	  val conf = new SparkConf().setAppName("Crimes").setMaster("local[1]")
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
		  var district = input._3.toDouble
		  var community = input._4.toDouble
		  (time,crimeType,district,community)
		})
		
	  // Initializing the RDD to assign the cluster to each point
		var closestTo = transformInput.map(x => (0,(x,0)))
		
		// Randomly initialize cluster centroids
		var clusters = transformInput.takeSample(false,10,System.nanoTime())
		
		
		var iterations = 20
	  var currentIteration = 1
	  val convergence = 0.05
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
		  
		  //calculateNewClusterCentroid = calculateNewClusterCentroid.sortBy(_._1)
		  
		  //println("Iteration number: " + currentIteration + "\n")
		  //calculateNewClusterCentroid.foreach(println)
		  
		  
		  println("\n\n\n\nIteration " + currentIteration + ": ")
		  
		  println("\n\nInitials Clusters: ")
		  clusters.foreach(println)
		  
		  println("\n\nCalculated: ")
		  calculateNewClusterCentroid.foreach(println)
		  
		  dist = 0.0
		  for (i <- 1 to 10) {
        dist += calculateDistance(clusters(i-1), calculateNewClusterCentroid(i))
      }
		  
		  
		  // Assign new cluster centroids
		  for(i <- 1 to clusters.length) {
		    clusters(i-1) = calculateNewClusterCentroid(i)
		  }
		  
		  println("\n\nNew Clusters: ")
		  clusters.foreach(println)

		  currentIteration+=1
		}
	  
	  //closestTo.foreach(println)
		
		//val vectors = transformInput.map(input => Vectors.dense(input._1,input._2,input._3,input._4))
	  
		
		sc.stop()
	}
}