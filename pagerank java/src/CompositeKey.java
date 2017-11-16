package hw3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class CompositeKey implements WritableComparable<CompositeKey>{

	//Composite Keys' variables
		private Text pageName = new Text();
		private DoubleWritable pr = new DoubleWritable();
		
		//Intialize compsosite Key
		
		public CompositeKey() {
			
		}
		
		public CompositeKey(String pageName,double pr) {
			this.pageName.set(pageName);
			this.pr.set(pr);
		}
		
		
		/*
		 * ----------------------------------------------------------------
		 * Getter and setter methods for primary key and secondary key
		 */
		public DoubleWritable getPageRank() {
			return pr;
		}

		public Text getPageName() {
			return pageName;
		}
		
	    public void setPageRank(double pr) {
	        this.pr.set(pr);
	    }

	    public void setPageName(String PageName) {
	        this.pageName.set(PageName);
	}
	/*---------------------------------------------------------------------	
	 */
		
		/*
		 * To serialize the data to send to reducer
		 */
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			pageName.write(out);
			pr.write(out);
			
		}
		/*
		 * To Deserialize thethe input data 
		 */
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			pageName.readFields(in);
			pr.readFields(in);
			
		}

		//Sorting first on the primary key "stationId" and then on the secondary key "year"
		public int compareTo(CompositeKey o) {
			//First compare with stationId i.e Primary Key
			int compareValue = o.getPageRank().compareTo(this.pr);
//			// compareValue = {-1,0,1}
//			if(compareValue==0) {
//				compareValue = this.pr.compareTo(o.getPageRank()); 
//			}
			return compareValue;
		}

	 
		   
		   @Override
		    public String toString() {
		        return pageName+"\t"+pr;
		}

	

	
}
