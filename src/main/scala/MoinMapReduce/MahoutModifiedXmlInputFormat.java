/*
Orginal Author/Source: Apache Mahout Source 
                       Slightly modifed by Moin Vahora 

The following class is a modified version of the Mahout XmlInputFormat.java file
Mahout's XmlInputFormat.java supports a single tag, this class
allows for support of multiple tags. The following 
Piazza thread led me to this solution (in the followup discussion, towards the end):
https://piazza.com/class/jzfqyf0i3twzp?cid=239
*/

package MoinMapReduce;

import java.io.IOException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapred.Reporter;
//import com.typesafe.confid.ConfigFactory;
//import org.apache.hadoop.conf.Configuration;




public class MahoutModifiedXmlInputFormat extends TextInputFormat {

    //from original Mahout file, not used here
    public static final String START_TAG_KEY = "xmlinput.start";
    public static final String END_TAG_KEY = "xmlinput.end";


  @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit Isplit, TaskAttemptContext config) { return new XmlRecordReader();}

    //---------------------------------------------------------
    public static class XmlRecordReader extends RecordReader<LongWritable, Text> {
        private byte[][] openingTags;
        private byte[][] closingTags;
        private long start;
        private long end;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable key = new LongWritable();
        private Text text = new Text();
        private int numTags;


        private int stopCheck(int[] allTags) {
            for (int i=0; i<numTags; i++) 
            {
                if (allTags[i] != 0)
                    return i;
            }
            return 0;
        }

        @Override
        public void initialize(InputSplit Isplit,  TaskAttemptContext config) throws IOException, InterruptedException {
            
            FileSplit split = (FileSplit) Isplit;
            

            //get tags to be processed 
            
            String[] openedTabsString = {"","","","","",""};
            openedTabsString[0]=config.getConfiguration().get("START_TAG1");
            openedTabsString[1]=config.getConfiguration().get("START_TAG2");
            openedTabsString[2]=config.getConfiguration().get("START_TAG3");
            openedTabsString[3]=config.getConfiguration().get("START_TAG4");
            openedTabsString[4]=config.getConfiguration().get("START_TAG5");
            openedTabsString[5]=config.getConfiguration().get("START_TAG6");
            
            String[] closedTabsString = {"","","","","",""};
            closedTabsString[0]=config.getConfiguration().get("END_TAG1");
            closedTabsString[1]=config.getConfiguration().get("END_TAG2");
            closedTabsString[2]=config.getConfiguration().get("END_TAG3");
            closedTabsString[3]=config.getConfiguration().get("END_TAG4");
            closedTabsString[4]=config.getConfiguration().get("END_TAG5");
            closedTabsString[5]=config.getConfiguration().get("END_TAG6");

            numTags=Integer.valueOf(config.getConfiguration().get("NUM_TAGS"));
          
            openingTags = new byte[numTags][];
            closingTags = new byte[numTags][];

            
            openingTags[0]=openedTabsString[0].getBytes("utf-8");
            openingTags[1]=openedTabsString[1].getBytes("utf-8");
            openingTags[2]=openedTabsString[2].getBytes("utf-8");
            openingTags[3]=openedTabsString[3].getBytes("utf-8");
            openingTags[4]=openedTabsString[4].getBytes("utf-8");
            openingTags[5]=openedTabsString[5].getBytes("utf-8");

            closingTags[0]=closedTabsString[0].getBytes("utf-8");
            closingTags[1]=closedTabsString[1].getBytes("utf-8");
            closingTags[2]=closedTabsString[2].getBytes("utf-8");
            closingTags[3]=closedTabsString[3].getBytes("utf-8");
            closingTags[4]=closedTabsString[4].getBytes("utf-8");
            closingTags[5]=closedTabsString[5].getBytes("utf-8");
      
            // open the file and seek to the start of the split
            start = split.getStart();
            end = start + split.getLength();
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(config.getConfiguration());
            fsin = fs.open(split.getPath());
            fsin.seek(start);


        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            int startCheck;
            int endCheck;
            if (fsin.getPos() < end) {
                //perform readuntillmatch for any of the specified tag
                startCheck = readUntilMatch(openingTags, false);
                if (startCheck>=0) { // Read until start_tag1 or start_tag2
                    try {

                        buffer.write(openingTags[startCheck - 1]);
                        //read all the contents before the end tag
                        endCheck = readUntilMatch(closingTags, true);
                        if (endCheck>=0) {
                            // updating the buffer with contents between start and end tags.
                            text.set(buffer.getData(), 0, buffer.getLength());
                            key.set(fsin.getPos());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return text;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (fsin.getPos() - start) / (float) (end - start);
        }

        @Override
        public void close() throws IOException {
            fsin.close();
        }

        private int readUntilMatch(byte[][] match, boolean withinBlock) throws IOException {
            int[] allTags = new int[numTags];

            while (true) {
                int b = fsin.read();
                // end of file:
                if (b == -1) return -1;
                // save to buffer:
                if (withinBlock) buffer.write(b);

                // check if we're matching any of the tag specified:
                for (int i = 0; i < numTags; i++) {
                    if (b == match[i][allTags[i]]) {
                        allTags[i]++;
                        if (allTags[i] >= match[i].length) 
                            return i+1;
                    } else 
                        allTags[i] = 0;
                }

                // see if we've passed the stop point:
                if (!withinBlock && stopCheck(allTags)==0 && fsin.getPos() >= end) return -1;
            }
        }

    }


}
