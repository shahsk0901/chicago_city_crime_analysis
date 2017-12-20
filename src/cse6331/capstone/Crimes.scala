package cse6331.capstone

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.io.FileWriter
import scala.collection.Seq
import org.apache.spark.broadcast.Broadcast

object Crimes {
  
  def calculateMinDist(clusteri: Broadcast[Array[((Double, Double, Double), Int)]],
      clusteriR: Broadcast[Array[(((String, String, Int), (Double, Double, Double)), Int)]],
      file: String) = {
    
    
    var cluster = clusteri.value
    var clusterRecords = clusteriR.value
    
    // Max and Min represents the maximum and the minimum distance from the cluster centroid
    var minTime = Double.PositiveInfinity
    var minCrimeType = Double.PositiveInfinity
    var minArea = Double.PositiveInfinity
    var minCommunity = Double.PositiveInfinity

    
    // Finding the highest crime type committed
    var crimeType = ""
    for(i <- 0 to clusterRecords.length -1) {
      var record = clusterRecords(i)
      var crimeTypeRatio = record._1._2._2 - cluster(0)._1._2
      if(crimeTypeRatio < minCrimeType) {
        minCrimeType = crimeTypeRatio
        crimeType = record._1._1._2
      }
    }
    var fw = new FileWriter(file,true)
    fw.write("\nCrime Type: " + crimeType)
	  fw.close()

	   // Finding most common time when the crime occurs
    var commonTime = ""
    for(i <- 0 to clusterRecords.length -1) {
      var record = clusterRecords(i)
      var commonTimeRatio = record._1._2._1 - cluster(0)._1._1
      if(commonTimeRatio < minTime) {
        minTime = commonTimeRatio
        commonTime = record._1._1._1
      }
    }
    fw = new FileWriter(file,true)
    fw.write("\nCommon Time: " + commonTime)
	  fw.close()

	   // Finding the community where most crime occurs
	  var community = 0.0
    for(i <- 0 to clusterRecords.length -1) {
      var record = clusterRecords(i)
      var communityRatio = record._1._2._3 - cluster(0)._1._3
      if(communityRatio < minCommunity) {
        minCommunity = communityRatio
        community = record._1._1._3
      }
    }
    fw = new FileWriter(file,true)
    fw.write("\nCommunity: " + community.toInt)
	  fw.close()
	  
  }
  
  
  
  
	def main(args: Array[String]) {
	  
	  
	  // Defining spark context and configuration
	  val conf = new SparkConf().setAppName("Crimes").setMaster("local[1]")
		val sc = new SparkContext(conf)
	  
	  def parallizeRecord(rec: (Double,Double,Double)): RDD[(Double,Double,Double)] = {
	    var x = sc.parallelize(Seq(rec))
	    x
	  }
	  
	  
	
  	// Hash function to uniquely convert string to hash code
    // Used for converting time, to avoid getting multiple values for a hash code.
    def knuthHash(word: String, constant: Int): Int = {
      var hash = 0
      for (ch <- word.toCharArray) {
        hash = ((hash << 5) ^ (hash >> 27)) ^ ch.toInt
      }
       
      hash % constant  
    } 
	  
  	// Calculating squared distance between a record and a cluster centroid
    // Calculates the similarity of record to a centroid.
    def calculateDistance(record: (Double,Double,Double),centroid: (Double,Double,Double)): Double = {      
      (Math.pow(record._1 - centroid._1, 2) + 
      Math.pow(record._2 - centroid._2, 2) + 
      Math.pow(record._3 - centroid._3, 2))
    }
	  
    // Calculating squared distance between a current cluster centroid and a updated cluster centroid
    // Calculated to measure convergence of the clusters
	  def calculateClusterDistance(record: (Double,Double,Double),centroid: (Int,(Double,Double,Double))): Double = {

      (Math.pow(record._1 - centroid._2._1, 2) + 
      Math.pow(record._2 - centroid._2._2, 2) + 
      Math.pow(record._3 - centroid._2._3, 2))
    }
	  
    // Adding records of each cluster to measure sum of values in each cluster (measuring the total weight of each cluster)
    def add(record1: (Double,Double,Double),record2: ((Double,Double,Double))) = {
	    (record1._1 + record2._1,
	    record1._2 + record2._2,
	    record1._3 + record2._3)
	  }
  
	  
    // Finding the cluster centroid which closest to the record
    // Returns the cluster ID closest to the record
  def closestToCluster(record: (Double, Double, Double), clusters: Array[(Double, Double, Double)]): Int = {
    
    var clusterAssigned = 0
		var minimumDistance = Double.PositiveInfinity

	  for (i <- 1 to clusters.length) {
		  var distanceToCentroid = calculateDistance(record, clusters(i-1))
				  if (distanceToCentroid < minimumDistance) {
					  minimumDistance = distanceToCentroid
							  clusterAssigned = i
				  }
	  }
    clusterAssigned
  }
	  
	  
    // Main K-means function 
    def runKmeans(k: Int,transformInput: RDD[(Double,Double,Double)], args: String): 
    (RDD[((Double,Double,Double),Int)],RDD[(Int)]) = { 
      
      println("\n\n\n\nK - means algorithm: Start\n\n\n\n")
      
  	  // Initializing the RDD to assign the cluster to each point
  		var closestTo = transformInput.map(x => (0,(x,0)))
  		
  		// Randomly initialize cluster centroids
  		var clusters = sc.parallelize(transformInput.takeSample(false,k,1000))
  		println("\n\n\n\n\n\n\n\nRandom clusters chosen for clustering\n\n\n\n")
  		clusters.foreach(println)
  		
  	  // keep count of iterations
  	  var currentIteration = 1
  	  val convergence = 0.1
  	  var dist = Double.PositiveInfinity
  		
  	  val broadcast = clusters.collect()
  	  val broadcastClusters = sc.broadcast(broadcast)
  	  
  	  
  	  while ( dist >  convergence) {
  		  
  	    println("\n\n\n\nIteration "+ currentIteration + ": Start\n\n\n\n")
  	    
  	    println("\n\n\n\nComputing closest distance of a record to the cluster centroids: Start\n\n\n\n")  	    
  	    closestTo = transformInput.map(record => {
  		    
  		    val clusters = broadcastClusters.value
  		    
  		    var clusterAssigned = closestToCluster(record, clusters)
  		    
  		    (clusterAssigned, (record,1))
  		  }).cache()
  		  println("\n\n\nClosest distance computation: End\n\n\n")
  		  
  		  
  		  println("\n\n\n\nComputing cluster weights to find convergence: Start\n\n\n\n")
  		  val calculateClusters = closestTo.reduceByKey {
  		    case ((record1,r1), (record2,r2)) => (add(record1,record2),r1+r2) 
  		  }
  		  
  		  calculateClusters.cache()
  		  
  		  println("\n\n\n\nCluster Weights Calculation: End\n\n\n\nCluster Weights as follows: \n\n\n\n")
  		  calculateClusters.foreach(println)
  		  
  		  // Computing new cluster centroids, from updated records and cluster assignment
  		  var calculateNewClusterCentroid = calculateClusters.map { 
  		    case(clusterIndex, (clusterInfo,totalRecords)) => (clusterIndex, (clusterInfo._1/totalRecords,
  		        clusterInfo._2/totalRecords,
  		        clusterInfo._3/totalRecords))
  		  }
  		  
  		  calculateNewClusterCentroid.cache()
  		  
  		  val broadcastcurrent = clusters.collect()
  		  val broadcastcurrentClusters = sc.broadcast(broadcastcurrent)
  		  val broadcastnew = calculateNewClusterCentroid.collect()
  		  val broadcastNewClusters = sc.broadcast(broadcastnew) 
  		  
  		  println("\n\n\n\nCurrent clusters: \n\n\n\n")
  		  clusters.foreach(println)
  		  
  		  // Calculates differences between the new calculated cluster centroids and the current clusters (measuring convergence)
  		  dist = 0.0
  		  for (i <- 1 to k) {
  		    var current = broadcastcurrentClusters.value
  		    var newClusters = broadcastNewClusters.value
  		    var temp = calculateClusterDistance(current(i-1), newClusters(i-1))
          dist += temp
        }
  		  
		  // Update cluster centroids with new calculated values
		  clusters = calculateNewClusterCentroid.map(f=> f._2)
		  println("\n\n\n\nUpdated clusters: \n\n\n\n")
		  clusters.foreach(println)
		  
		  currentIteration+=1
		  
		  println("\n\n\n\nIteration "+ (currentIteration-1) + ": End\n")
	  }
	  		
		// Assigning cluster ID's to each cluster
		var i = 0
		val clust = clusters.map(f=> {
		  i+=1
		  ((f._1,f._2,f._3),i)
		})
		
		(clust,closestTo.map(f=>f._1))
  }
	
  
  
  
  
  
	  
	  
	  //---------------------------------------------------------------------------------------------------------------------------------
	  
	  
	  var fw = new FileWriter(args(2),true)
	  fw.write("\nProgram Start\n---------------------------------------------------------------------------------------------------------")
	  fw.close()
	 
		// Reading Input and filtering out any rows that has a null value
	  var fileInput = sc.textFile(args(0))
	  
	  fw = new FileWriter(args(2),true)
	  fw.write("\n\nRecords Read: " + fileInput.count())
	  fw.close()
		
	  var input = fileInput.map(line=>{
		  var x = line.split(",") 
		  
		  // Drop Rows where all values not present
		  if(x.length == 4) {
		    
		    // Transforming time dimension to proper value
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
		    
		    (finalTime,x(1).toString(),x(3).toInt)
		  } else {
		    
		    (null,null,null.asInstanceOf[Int])
		  }
		}).filter(x => x._1 != null)
		
		var transformInput = input.map(input => {
		  var time = knuthHash(input._1, Int.MaxValue).toDouble
		  var crimeType = input._2.map(_.toByte.doubleValue()).reduceLeft((x,y) => x+y)
		  var community = input._3.toDouble
		  
		  (time,crimeType,community)
		})
		
		fw = new FileWriter(args(2),true)
	  fw.write("\n\nRecords Filtered: " + (fileInput.count() - input.count()))
	  fw.write("\n\nTotal Records for K-means: " + transformInput.count())
	  fw.close()
	  
		
		var returnValue = runKmeans(10,transformInput, args(1) + "/clusters")
		
		val clusters = returnValue._1
		val clusterAssignedToRecords = returnValue._2
		
		val records = input.zip(clusterAssignedToRecords)
	
		val temp = input.zip(transformInput)
		val result = temp.zip(clusterAssignedToRecords)
		
		fw = new FileWriter(args(2),true)
	  fw.write("\n\nWriting -> clusters computed to 'output/clusters'")
	  fw.write("\nWriting -> records & cluster prediction to 'output/records'")
	  fw.close()
		
	  println("\n\nFinal Clusters Computed: \n\n")
		clusters.foreach(println)
		clusters.saveAsTextFile(args(1) + "/clusters")
		records.saveAsTextFile(args(1) + "/records")
		println("\n\nAll Records saved to file\n\n")
		
		
		for(i <- 1 to 10) {
		  var clusteri = clusters.filter(f=> f._2 == i)
  		var clusteriRecords = result.filter(f=> f._2 == i)
  		
    	var fw = new FileWriter(args(2),true)
      fw.write("\n\nPrinting results for cluster " + i + ": ")
  	  fw.close()
  		
  	  var bclusi = clusteri.collect()
  	  var bci = sc.broadcast(bclusi)
  	  var bclusiR = clusteriRecords.collect()
  	  var bciR = sc.broadcast(bclusiR)
  	  
  		calculateMinDist(bci,bciR,args(2))
		}
		
		sc.stop()
		
		
	  fw = new FileWriter(args(2),true)
	  fw.write("\n\n---------------------------------------------------------------------------------------------------\nProgram Stop")
	  fw.write("\nLogs written to '.out' file.")
	  fw.close()
	  
	}
}