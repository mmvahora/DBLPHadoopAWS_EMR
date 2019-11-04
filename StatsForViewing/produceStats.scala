/*
Moin Vahora

This progam ticks the "vizualzation" requirment of the required homework functionality by producing csv files/tables with
various stats, see:
https://piazza.com/class/jzfqyf0i3twzp?cid=256
This is a simple program which will open the "part-r-00000" file which was produced from 
the M/R job. Make sure the file is name exactly "part-r-00000" and is in
the same directory as this file and the makefile

This program opens the file, reads each line, records data while reading,
and then finally outputs multiple (9) .csv files 
*/

import scala.xml.XML
import scala.io.{Source, BufferedSource}
import java.nio.file.{Files, Path, Paths}
import java.io._


object produceStats {
  
  //class for an entry, each entry has an author name, score, coauthor avg, coauthor min, and total entries
  case class Person(name: String, score: Double, avg: Double, max: Int, min: Int, tot: Int)

  def main(args: Array[String]): Unit = {

    //some vars are needed to build stats
    var test = List[Person]()

    //open and reach file
    val filename = "part-r-00000"
    
    val source: BufferedSource = scala.io.Source.fromFile(filename)

    //bins for histograms
    var bin1=0;
    var bin2=0;
    var bin3=0;
    var bin4=0;
    var bin5=0;

    var abin1=0;
    var abin2=0;
    var abin3=0;
    var abin4=0;
    var abin5=0;

    //read each line of file
    for (line <- source.getLines) 
    {
      //parse for each value, save into val
      val entry = line.split(",").map(_.trim).toList

      val it = entry.iterator
      val name = it.next().toString
      val score = it.next().toDouble
      val avg = it.next().toDouble
      val max=it.next().toInt
      val min=it.next().toInt
      val tot=it.next().toInt
      
      //the following if statements deal with building stats for both histograms, bins partioned manually
      if (score <=85 )
      {
        bin1=bin1+1
      }

      if (score > 85 && score <=170 )
      {
        bin2=bin2+1
      }

      if (score > 170 && score <=255 )
      {
        bin3=bin3+1
      }

      if (score > 255 && score <=340 )
      {
        bin4=bin4+1
      }

      if (score > 340 )
      {
        bin5=bin5+1
      }

       if (tot <=328 )
      {
        abin1=abin1+1
      }

      if (tot > 329 && tot <=656 )
      {
        abin2=abin2+1
      }

      if (tot > 656 && tot <=984 )
      {
        abin3=abin3+1
      }

      if (tot > 985 && tot <=1312 )
      {
        abin4=abin4+1
      }

      if (tot > 1313 )
      {
        abin5=abin5+1
      }

      //build a person from the entry stats, append to list
      test =Person(name, score, avg, max, min, tot) :: test
      
    }
    //close file once done
    source.close

    //take the list and make sorted lists from them based on the stat we want to sort by
    val sorted=test.sortBy(x => (x.score)) 
    val sorted2= sorted.reverse 
    val sorted3= test.sortBy(x => (x.min)).reverse
    val sorted4=test.sortBy(x => (x.max)).reverse 
    val sorted5=test.sortBy(x => (x.tot)).reverse
    val sorted6=test.sortBy(x => (x.avg)).reverse 
    val sorted7=test.sortBy(x => (x.avg))

    //here are the 9 files we will produce 
    val file = new File("bot100AuthorshipScores.csv")
    val bw = new BufferedWriter(new FileWriter(file))

    val file2 = new File("top100AuthorshipScores.csv")
    val bw2 = new BufferedWriter(new FileWriter(file2))

    val file3 = new File("top100LeastCoAuthors.csv")
    val bw3 = new BufferedWriter(new FileWriter(file3))

    val file4 = new File("top100MostCoAuthors.csv")
    val bw4 = new BufferedWriter(new FileWriter(file4))

    val file5 = new File("top100TotalEntries.csv")
    val bw5 = new BufferedWriter(new FileWriter(file5))
    
    val file6 = new File("top100AvgCoAuthors.csv")
    val bw6 = new BufferedWriter(new FileWriter(file6))

    val file7 = new File("bot100AvgCoAuthors.csv")
    val bw7 = new BufferedWriter(new FileWriter(file7))

    val file8 = new File("authorshipScoreHistogram.csv")
    val bw8 = new BufferedWriter(new FileWriter(file8))

    val file9 = new File("totalEntriesHistogram.csv")
    val bw9 = new BufferedWriter(new FileWriter(file9))

    //bins have been processed already, so we can build and write the histogram CSVs first
    bw8.write("0-85" + ", " + bin1 + "\n")
    bw8.write("86-170" + ", " + bin2 + "\n")
    bw8.write("171-255" + ", " + bin3 + "\n")
    bw8.write("256-340" + ", " + bin4 + "\n")
    bw8.write("341-425" + ", " + bin5 + "\n")

    bw9.write("0-328" + ", " + abin1 + "\n")
    bw9.write("329-656" + ", " + abin2 + "\n")
    bw9.write("657-984" + ", " + abin3 + "\n")
    bw9.write("985-1312" + ", " + abin4 + "\n")
    bw9.write("1313-1640" + ", " + abin5 + "\n")

    

    //make an iterator for each sorted list, this is for top/bot 100 stats
    var count = 1;
    val finalIt= sorted.iterator
    val finalIt2= sorted2.iterator
    val finalIt3= sorted3.iterator
    val finalIt4= sorted4.iterator
    val finalIt5= sorted5.iterator
    val finalIt6= sorted6.iterator
    val finalIt7= sorted7.iterator
    while (count <= 100)
    {
      //get the 1st 100 values from each sorted list and write to their respective file
      val user = finalIt.next()
      val user2 = finalIt2.next()
      val user3 = finalIt3.next()
      val user4 = finalIt4.next()
      val user5 = finalIt5.next()
      val user6 = finalIt6.next()
      val user7 = finalIt7.next()
      bw.write(user.name +", " +user.score + "\n")
      bw2.write(user2.name  +", " +user2.score +"\n")
      bw3.write(user3.name  +", " +user3.min +"\n")
      bw4.write(user4.name  +", " +user4.max + "\n")
      bw5.write(user5.name  +", " +user5.tot + "\n")
      bw6.write(user4.name  +", " +user6.avg + "\n")
      bw7.write(user5.name  +", " +user7.avg + "\n")
      count=count+1
      
    }

    //close all files to free resources and write
    bw.close()
    bw2.close()
    bw3.close()
    bw4.close()
    bw5.close()
    bw6.close()
    bw7.close()
    bw8.close()
    bw9.close()


  }
}