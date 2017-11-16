package hw3;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.SAXException;
/*
 * Created by Shabbir Saifee
 * purpose: This class generates key(page name) and vale (outlinks) for each node
 */
public class PreprocessMapper extends Mapper<Object, Text, Text, Text>{

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
			
        try {
        
        	// Parsing each line and getting Page object
            Page pg = Bz2WikiParser.parseHTML(value.toString());
            if (pg != null) {

            	// Adds those nodes to data set which are present but not in data set
                for (String links : pg.adjacenctPages){
                    context.write(new Text(links), new Text());
                }
                
                // handles condition for sink nodes
                context.write (new Text (pg.pageName), new Text ());
                context.write(new Text(pg.pageName), new Text(pg.adjacenctPages.toString()));
            }

        } catch (SAXException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
}
		
	}
	
	
}
