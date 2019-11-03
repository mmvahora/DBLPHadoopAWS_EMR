import scala.xml.XML
import scala.io.{Source, BufferedSource}
import java.nio.file.{Files, Path, Paths}
import java.io._


object produceStats {
  
  case class Person(name: String, score: Double, avg: Double, max: Int, min: Int, tot: Int)

  def main(args: Array[String]): Unit = {


    var test = List[Person]()
    val filename = "part-r-00000"
    
    val source: BufferedSource = scala.io.Source.fromFile(filename)
    var c=0

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

    for (line <- source.getLines) 
    {
      val entry = line.split(",").map(_.trim).toList
      c=c+1

      
      val it = entry.iterator
      val name = it.next().toString
      val score = it.next().toDouble
      val avg = it.next().toDouble
      val max=it.next().toInt
      val min=it.next().toInt
      val tot=it.next().toInt
      
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

      test =Person(name, score, avg, max, min, tot) :: test
      
    }
    source.close


    val sorted=test.sortBy(x => (x.score)) //works
    val sorted2= sorted.reverse //works
    val sorted3= test.sortBy(x => (x.min)).reverse
    val sorted4=test.sortBy(x => (x.max)).reverse //works
    val sorted5=test.sortBy(x => (x.tot)).reverse
    val sorted6=test.sortBy(x => (x.avg)).reverse //works
    val sorted7=test.sortBy(x => (x.avg))

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