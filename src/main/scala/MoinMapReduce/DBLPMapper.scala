/*
Moin Vahora

Here is my Mapper class for the Map/Reduce hadoop job
Mapper outputs <Key, Value> pairs as <Text, Text> so I can store
both the author's name, but also so I can build multiple stats into a string
and write that, rather than writing a single stat

The current stats produced by the mapper are all relative to the Author Name:

Authorship Score
Average CoAuthors per entry
Max coauthors in a paper
Min Coauthors in a paper
Total publications 

*/

package MoinMapReduce

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.xml.XML


class DBLPMapper extends Mapper[LongWritable, Text, Text, Text] {

  val myLogger: Logger = LoggerFactory.getLogger(this.getClass)
  val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI

  //parse xml for each author
  def parseAuthors(myXML: String): List[String] = {
    
    //credit to Amna Irfan's piazza post which led me to this line of code
    // https://piazza.com/class/jzfqyf0i3twzp?cid=253
    val newXML = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n<!DOCTYPE dblp SYSTEM \""+getClass.getClassLoader.getResource("dblp.dtd").toURI.toString+"\">" + myXML
    myLogger.info("XML to parse: " + newXML)

    val entry = XML.loadString(newXML)

    //build list from parsed authors, making sure to convert to lowercase for consistency reasons
    val authorList = (entry \\ "author").map(name => name.text.toLowerCase.trim).toList

    authorList
  }

  def creditScores(scores: List[Double]):List[Double] ={

    val credit = 1.0/(4*scores.length)
    myLogger.info("Credit amount: " + credit)

    var temp=0.0
    var test = List[Double]()

    val it=scores.iterator

    //make sure iterator is not empty before crediting
    if (it.hasNext)
    {
          temp = it.next()
    
    if (it.hasNext)
    {
      temp = temp - credit
      test = temp :: test
    }
    else 
    {
      test = temp :: test
    }

    while (it.hasNext)
    {
      temp = it.next() + credit
      if (it.hasNext)
      {
        temp = temp - credit
        test = temp :: test
      }
      else
      {
        test = temp :: test
      }
    }
    }
    
    //return list of credited scores
    test
  }

  //map function, I chose to have my <Key, Value> pair as a <Text, Text> to store multiple stats
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {


    //get authors from xml
    val parsedAuthors = parseAuthors(value.toString)

    //set initScore and credit score
    val initScore = 1.0/parsedAuthors.length
    val creditScore = 1.0/(4*parsedAuthors.length)

    //fill list with scores pre-credit
    val scores = List.fill(parsedAuthors.length)(initScore)

    //get scores after crediting is done
    val creditedScores = creditScores(scores)

    myLogger.info("initScore: " + initScore)
    myLogger.info("creditScore: " + creditScore)

    //zip together authors and their authorship scores
    val combined = parsedAuthors zip creditedScores

    //for each author, write their name is a key, their stats as value (Text)
    for ((i, j) <- combined)
    {
      //build string of stats
      val stats= ", " +j + ", " + parsedAuthors.length  + ", " +parsedAuthors.length + ", " +parsedAuthors.length + ", " +1
      
      myLogger.info("Writing Name: " + i + "Writing Stats: " + j)
      context.write(new Text(i), new Text(stats))
    }


  }


}
