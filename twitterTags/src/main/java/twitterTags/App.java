package twitterTags;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class App 
{
    public static void main( String[] args )
    {
    	topTwitterTags(20);
    }
    
    public static void topTwitterTags(int numOfTags) {
    	Logger.getLogger("org").setLevel(Level.ERROR);
    	
    	// CREATE SPARK CONTEX
    	SparkConf conf = new SparkConf ().setAppName ("wordCounts").setMaster ("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext (conf);
        
        
        // LOAD DATASETS
        JavaRDD<String> videos = sparkContext.textFile ("src/main/resources/USvideos.csv");
 
        
        // TRANSFORMATIONS
        JavaRDD<String> tags = videos
                .map (v -> {
                	String[] data = v.split(",");
                	return data.length > 6 ? data[6]: "" ;
                }).filter(t -> StringUtils.isNotBlank(t) && !t.equals("[none]"));
        
        
       // JavaRDD<String>
        JavaRDD<String> tagList = tags.flatMap (t -> Arrays.asList (t
                .toLowerCase ()
                .trim ().split("\\|")).iterator ());
        System.out.println("********************* Top "+ numOfTags +" Tags on Twitter ********************");
        // COUNTING
        Map<String, Long> tagCounts = tagList.countByValue ();
        List<Map.Entry> sorted = tagCounts.entrySet ().stream ()
                .sorted (Map.Entry.comparingByValue (Comparator.reverseOrder()))
                .limit(numOfTags).collect (Collectors.toList ());
        
        // DISPLAY
        sorted.stream().forEach(entry -> System.out.println (entry.getKey () + " : " + entry.getValue ()));
    	
    }
}
