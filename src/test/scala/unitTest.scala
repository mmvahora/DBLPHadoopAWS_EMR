/*
Moin Vahora

Scala unit tests

No comments, should be self explanatory just from unit test naming
*/

import MoinMapReduce.hw2Mapper
import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec

class unitTest extends FlatSpec {
    val conf = ConfigFactory.load("tagConfig")
    val mapper = new hw2Mapper

    "Load config" should "load name" in 
    {
        val test = conf.getString("JOB_NAME")
        assert(test.length > 0)
    }

    
    "Number of Tags from config" should "return 6" in 
    {
        val test = conf.getString("NUM_TAGS")
        assert(test.toInt == 6)
    }

    "Parse authors function" should "return list of authors in entry" in
    {
        val testXML = "    <inproceedings mdate=\"2017-05-24\" key=\"conf/icst/GrechanikHB13\">\n<author>Ugo Buy</author>\n<author>B. M. Mainul Hossain</author>\n<author>Mark Grechanik</author>\n<title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>\n<pages>174-183</pages>\n<year>2013</year>\n<booktitle>ICST</booktitle>\n<ee>https://doi.org/10.1109/ICST.2013.19</ee>\n<ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>\n<crossref>conf/icst/2013</crossref>\n<url>db/conf/icst/icst2013.html#GrechanikHB13</url>\n</inproceedings>"
        val test =  mapper.parseAuthors(testXML)
        assert(test.length == 3)
    }

    "Check dtd file" should "load the dtd filepath and check if it exits" in
    {
        assert(getClass.getClassLoader.getResource("dblp.dtd").toURI.toString.length > 0)
    }

    "Credit scores" should "return a list whose sum adds up to 1.0" in 
    {
        val scores = List.fill(4)(0.25)
        val creditedScores = mapper.creditScores(scores)
        assert(creditedScores.sum == 1.0)
    }
}