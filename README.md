# Moin Vahora
# Map/Reduce in Hadoop

**In this assignment, we run a Map/Reduce job in Hadoop on the dblp.xml database. My Mapper is located in DBLPMapper.scala, and my job Driver and Reducer class located in Begin.scala. The sharding of the database file is done by Hadoop framework, independent of the user. The mapper reads in the XML, parses for authors,and then builds stats based on the entry. The <Key, Value> pair of my M/R job is <Text, Text> so I can store multiple comma separated values. The Key will be the author name, the Value will be AuthorshipScore, AvgCoAuthors, MaxCoAuthors, MinCoAuthors, and TotalEntries.  For more detailed information on code and logic, refer to in-file comments.**

**Folder "/StatsForViewing/CSVStats/" holds my csv stat files, as well as a compressed "part-r-00000" file (part-r-00000.tar.gz) in case user would like to view stats without running entire M/R job. This file was produced from running the job locally on my HortonWorks VM**

# AWS EMR Deployment video:
 https://www.youtube.com/watch?v=nB1YE4yIUEc

**To run entire M/R Program follow below, continue reading**

# Instructions to run Hadoop M/R Job and produce .csv stat files from job's output:

# PreReqs Needed:
   Java JDK 1.8 + SBT (SimpleBuildTools)

# 1.) 
run 

    sbt assembly

   This will compile code, run unit tests, and produce a jar executable
   **NOTE: This command might take a few seconds to run**

# 2.) 
Take jar file 

     DBLPJob-assembly-1.0.jar
 located in /target/scala-2.12
and move to environment with hadoop (your VM)

# 3.) 
run 
    
    chmod 777 DBLPJob-assembly-1.0.jar 
   This is to give the jar the needed access rights

# 4.) 
run 

    hadoop jar DBLPJob-assembly-1.0.jar MoinMapReduce.Begin [DBLP.xmlInput_Directory] [OUTPUT_DIRECTORY] 

  An example would like this:

    hadoop jar DBLPJob-assembly-1.0.jar MoinMapReduce.Begin /tmp/data /tmp/output

   This will run the M/R job and produce a file:

    part-r-00000 
This file contains <Key, Value> pairs
    as <Text, Text>, where the Key is the author name, and the Value is 5 comma separated stats(in order):

        AuthorshipScore (Double)
        AvgCoAuthors (Double)
        MaxCoAuthors (Int)
        MinCoAuthors (Int)
        TotalEntries (Int) 

   Feel free to look over this file and stats before moving forward
    
# 5.) 
Once job is done, take the produced 

    part-r-00000
 file (make sure the file is named exactly like that, "part-r-0000", rename if not) and move it to 

    DBLPJob/StatsForViewing/ 

there should be
    
    produceStats.scala 
and a 
    
    makefile
in the same directory 

   **NOTE: Again, I have provided a compressed 
"part-r-00000" file (part-r-00000.tar.gz) in the "StatsForViewing/CSVStats" folder for convience reasons. Feel free to use this file if you would like to save time/only care about the statistics produced.** 

# 6.) 
Once the "part-r-00000" is in the same directory as "produceStats.scala"
    run the command: "make build" to compile
    next, run the command: "make run" 

   **NOTE: This command might take a few seconds to run**

7.) 9 csv visualizations files should be produced in the same directory from the previous command:

    authorshipScoreHistogram.csv
    totalEntriesHistogram.csv
    top100LeastCoAuthors.csv
    top100MostCoAuthors.csv
    top100TotalEntries.csv
    top100AuthorshipScores.csv
    top100AvgCoAuthors.csv
    bot100AuthorshipScores.csv
    bot100AvgCoAuthors.csv

# IntelliJ Import:
1.) To import project into IntelliJ, first clone repo.

2.) Open IntelliJ, select "Import Project"

3.) Select directory "DBLPHadoopAWS_EMR"

4.) Select "Import project from extrernal model", select "SBT" and press next

5.) Make sure Project JDK is set to 1.8, then press finish.

6.) IntelliJ should build project with no problems. 
