/*
Moin Vahora
Combined driver and reducer class
*/

package MoinMapReduce

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import java.lang


//our driver object, this object will be called to  begin Hadoop M/R job
object Begin {

  //main function, considered the "driver" 
  //Here we set up the functionality and parameters of the M/R job
  //and load needed info from our config file
  def main(args: Array[String]): Unit = {
    
    val myLogger: Logger = LoggerFactory.getLogger(this.getClass)
    val xmlConfig = new Configuration
    val mainConf = ConfigFactory.load("tagConfig")

    //load all the start and end tags from config file
    //set Configuration to values from Config to allow us to
    //access config file values else where in MahoutModified
    xmlConfig.set("NUM_TAGS", mainConf.getString("NUM_TAGS"))
    xmlConfig.set("START_TAG1", mainConf.getString("START_TAG1"))
    xmlConfig.set("START_TAG2", mainConf.getString("START_TAG2"))
    xmlConfig.set("START_TAG3", mainConf.getString("START_TAG3"))
    xmlConfig.set("START_TAG4", mainConf.getString("START_TAG4"))
    xmlConfig.set("START_TAG5", mainConf.getString("START_TAG5"))
    xmlConfig.set("START_TAG6", mainConf.getString("START_TAG6"))

    xmlConfig.set("END_TAG1", mainConf.getString("END_TAG1"))
    xmlConfig.set("END_TAG2", mainConf.getString("END_TAG2"))
    xmlConfig.set("END_TAG3", mainConf.getString("END_TAG3"))
    xmlConfig.set("END_TAG4", mainConf.getString("END_TAG4"))
    xmlConfig.set("END_TAG5", mainConf.getString("END_TAG5"))
    xmlConfig.set("END_TAG6", mainConf.getString("END_TAG6"))

    //help from here https://www.programcreek.com/java-api-examples/index.php?api=org.apache.hadoop.io.serializer.JavaSerializationComparator
    xmlConfig.set(mainConf.getString("IO"), mainConf.getString("IOset"));
    
    //start job
    val mapReduceJob = Job.getInstance(xmlConfig, mainConf.getString("JOB_NAME"))
   
    mapReduceJob.setJarByClass(this.getClass)

    //Here we set up our mapper
    mapReduceJob.setMapperClass(classOf[DBLPMapper])
    myLogger.info("Mapper set")

    //Use MahoutModifiedXMLInputFormat as XML input format class
    mapReduceJob.setInputFormatClass(classOf[MahoutModifiedXmlInputFormat])
    mapReduceJob.setCombinerClass(classOf[simpleReducer])

    //here we set up our reducer
    mapReduceJob.setReducerClass(classOf[simpleReducer])
    myLogger.info("Reducer set")

    //my <Key, Value> pair is set to the form <Text, Text>
    mapReduceJob.setOutputKeyClass(classOf[Text])
    mapReduceJob.setOutputValueClass(classOf[Text])
    myLogger.info("<Key, Value> pair set to <Text, Text>")

    //set input and output paths based on user command line arguments 
    FileInputFormat.addInputPath(mapReduceJob, new Path(args(1)))
    FileOutputFormat.setOutputPath(mapReduceJob, new Path(args(2)))
    myLogger.info("Input and output set")
    
    //Done
    if (mapReduceJob.waitForCompletion(true))
    {
      myLogger.info("Exit code 0")
      System.exit(0)
    }
    else
    {
      myLogger.info("Exit code 1")
      System.exit(1)
    }


  }
}


/*
simpleReducer is our Reducer class
This class expects a <KeyIN, ValueIN> of <Text, Text> (my KV pair setting)
and also expects to write a <KeyOUT, ValueOUT> in the same form as above

KeyIn will be the name of the author
ValueIN will be Text stats

My reducer will parse this Text ValueIN, caluclate the various stats
from iterarating the list of values, and the rebuild a string to write
*/
class simpleReducer extends Reducer[Text, Text, Text, Text] {

  //logger
  val myLogger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {

    myLogger.info("Begin Reduce")
    //NOTE: THESE ARE THE ONLY VARS
    //vars are neeeded here to build stats from Text value
    //values must be updated after every Text value is read
    //to be able to get the needed stats
    var score = 0.0
    var avg = 0.0
    var temp = 0
    var max= 1
    var min=1
    var totPub=0
    var count=0

    //for each Text field in values, split the stats, do computation, save stats to be written
    for (stats <- values)
    {
      val split = stats.toString.split(",").map(_.trim).toList
      val it = split.iterator

      //deal with empty string
      it.next()
      //start adding up authorship scores across entries 
      score = score+ it.next().toDouble
     
      //start adding averages 
      avg = avg+ it.next().toDouble
 
      //the following lines deal with finding the max and min coauthors. These value will always be at least 1
      temp = it.next().toInt
      if (temp > max)
      {
        myLogger.info("New Max " +temp + "for " +key.toString)
        max = temp
      }
      
      temp = it.next().toInt
      if (temp < min)
      {
        myLogger.info("New Min " +temp + "for " +key.toString)
        min = temp
      }

      //get total number of times the author shows up in dataset
      totPub = totPub + it.next().toInt

      //count for calculating average at end
      count= count+1

    }

    //calculate average
    avg = avg/count.toDouble
    
    //build reduced stats string
    val reducedStats= ", " + score + ", " + avg + ", " +max + ", " +min + ", " +totPub
    
    myLogger.info("Key: "+ key.toString + "   Stats: " + reducedStats)
    //write key and value
    context.write(key, new Text(reducedStats))
  }
}