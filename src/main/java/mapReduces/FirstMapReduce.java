package mapReduces;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Job;
import com.amazonaws.samples.Bigram;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

// This map reduce is filtering stop words (eng/heb),
// and calculating C(w1,w2) for each pair by decade.

public class FirstMapReduce {
	//public FirstMapReduce() {}
	public static class FirstMapReduceMapper extends Mapper<LongWritable, Text, Bigram, LongWritable> {
		final String[] EnglishStopWords = {"a","about","above","across","after","afterwards","again","against","all","almost","alone","along","already","also","although","always","am","among","amongst","amoungst","amount","an","and","another","any","anyhow","anyone","anything","anyway","anywhere","are","around","as","at","back","be","became","because","become","becomes","becoming","been","before","beforehand","behind","being","below","beside","besides","between","beyond","bill","both","bottom","but","by","call","can","cannot","cant","co","computer","con","could","couldnt","cry","de","describe","detail","do","done","down","due","during","each","eg","eight","either","eleven","else","elsewhere","empty","enough","etc","even","ever","every","everyone","everything","everywhere","except","few",
				"fifteen","fify","fill","find","fire","first","five","for","former","formerly","forty","found","four","from","front",
				"full","further","get","give","go","had","has","hasnt","have","he","hence","her","here","hereafter",
				"hereby","herein","hereupon","hers","herself","him","himself","his","how","however","hundred","i","ie","if","in",
				"inc","indeed","interest","into","is","it","its","itself","keep","last","latter","latterly","least","less","ltd","made",
				"many","may","me","meanwhile","might","mill","mine","more","moreover","most","mostly","move","much","must","my","myself",
				"name","namely","neither","never","nevertheless","next","nine","no","nobody","none","noone","nor","not","nothing","now","nowhere",
				"of","off","often","on","once","one","only","onto","or","other","others","otherwise","our","ours","ourselves","out","over","own",
				"part","per","perhaps","please","put","rather","re","same","see","seem","seemed","seeming","seems","serious","several","she",
				"should","show","side","since","sincere","six","sixty","so","some","somehow","someone","something","sometime","sometimes",
				"somewhere","still","such","system","take","ten","than","that","the","their","them","themselves","then","thence","there",
				"thereafter","thereby","therefore","therein","thereupon","these","they","thick","thin","third","this","those","though","three",
				"through","throughout","thru","thus","to","together","too","top","toward","towards","twelve","twenty","two","un","under","until",
				"up","upon","us","very","via","was","we","well","were","what","whatever","when","whence","whenever","where","whereafter","whereas",
				"whereby","wherein","whereupon","wherever","whether","which","while","whither","who","whoever","whole","whom","whose","why","will",
				"with","within","without","would","yet","you","your","yours","yourself","yourselves","—","|","§","~","¡","°","¿","•","<",">",";",":","^",
				"[","]","(",")","\\","«",""};
		final String[] HebrewStopWords ={"״","׳","של","רב","פי","עם","עליו","עליהם","על","עד","מן","מכל","מי","מהם","מה","מ","למה","לכל","לי","לו","להיות","לה","לא","כן","כמה","כלי","כל","כי","יש","ימים","יותר","יד","י","זה","ז","ועל","ומי","ולא","וכן","וכל","והיא","והוא","ואם","ו","הרבה","הנה","היו","היה","היא","הזה","הוא","דבר","ד","ג","בני","בכל","בו","בה","בא","את","אשר","אם","אלה","אל","אך","איש","אין","אחת","אחר","אחד","אז","אותו","־","^","?",";",":","1",".","-","*","\"","!","שלשה","בעל","פני",")","גדול","שם","עלי","עולם","מקום","לעולם","לנו","להם","ישראל","יודע","זאת","השמים","הזאת","הדברים","הדבר","הבית","האמת","דברי","במקום","בהם","אמרו","אינם","אחרי","אותם","אדם","(","חלק","שני","שכל","שאר","ש","ר","פעמים","נעשה","ן","ממנו","מלא","מזה","ם","לפי","ל","כמו","כבר","כ","זו","ומה","ולכל","ובין","ואין","הן","היתה","הא","ה","בל","בין","בזה","ב","אף","אי","אותה","או","אבל","א","—","|"};//u dont nee to add here i think man ok ok ok sim gitarot only english hhhh
		//public FirstMapReduceMapper() {}

		/*
		protected void setup(Context context) throws IOException, InterruptedException {
			isStopWordsIncluded = Integer.parseInt(context.getConfiguration().get("isStopWordsIncluded")) == 1? true : false;
		}
		*/
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer valueIterator = new StringTokenizer(value.toString());

			if(valueIterator.countTokens() == 6) { 
				String str1 = valueIterator.nextToken();
				Text first= getTextWord(str1);
				String str2 = valueIterator.nextToken();
				Text second = getTextWord(str2);
				
				boolean exist = false;
				if (context.getConfiguration().get("language").equals("eng")) {
					str1 = str1.trim().toLowerCase();
					str2 = str2.trim().toLowerCase();
					exist=doesStopWordExist(EnglishStopWords,str1,str2);
					if (!exist) {
						writeToContext(context, valueIterator, first, second);    
					}
				}
				else {
					exist=doesStopWordExist(HebrewStopWords,str1,str2);
					if (!exist) {
						writeToContext(context, valueIterator, first, second); 
					}
				}
			} 
		}
		
		private Text getTextWord(String str) {	
			//if (str.contains("_"))   step111.jar
				//str = str.substring(0, str.indexOf("_"));
			return new Text(str);
		}

		private static boolean doesStopWordExist(String[] StopWords, String str, String str2) {
			for (int i = 0; i < StopWords.length; i++) {
				if (StopWords[i].equals(str) || str.equals("") || StopWords[i].equals(str2) || str2.equals("")) 
					return true;
			}
			return false;
		}    

		private static void writeToContext(Context context, StringTokenizer itr, Text first, Text second) throws NumberFormatException, IOException, InterruptedException {
			Text year = new Text(itr.nextToken());												
			Text decade = getDecade(year);
			Text occurrences = new Text(itr.nextToken());
			Bigram bigram = new Bigram(first, second, decade);
			context.write(bigram, new LongWritable(Integer.parseInt(occurrences.toString())));
		}
		
		private static Text getDecade(Text yearStr) {
			int yearInt=Integer.parseInt(yearStr.toString());
			//int decade= (yearInt / 10) * 10;
			int decade= yearInt - (yearInt %10);
			Text ans =new Text(String.valueOf(decade));
			return ans;
		}
	}


public static class FirstMapReduceReducer extends Reducer<Bigram,LongWritable,Bigram,LongWritable> {
	@Override
	public void reduce(Bigram key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable value : values) 
			sum += value.get();
		
		context.write(key, new LongWritable(sum));
	}
}

public static void main(String[] args) throws Exception, ClassNotFoundException, InterruptedException  {
	Configuration conf = new Configuration();
	conf.set("language", args[2]);
	Job myJob = new Job(conf, "step1");
	myJob.setJarByClass(FirstMapReduce.class);
	myJob.setMapperClass(FirstMapReduceMapper.class);
	//myJob.setCombinerClass(FirstMapReduceReducer.class);
	myJob.setReducerClass(FirstMapReduceReducer.class);
	myJob.setOutputKeyClass(com.amazonaws.samples.Bigram.class); 
	myJob.setOutputValueClass(LongWritable.class);
	myJob.setOutputFormatClass(TextOutputFormat.class);
	myJob.setInputFormatClass(SequenceFileInputFormat.class);
	//myJob.setInputFormatClass(TextInputFormat.class); // When using encoded file, use SequenceInputFormat
	myJob.setMapOutputKeyClass(com.amazonaws.samples.Bigram.class);
    myJob.setMapOutputValueClass(LongWritable.class);
	SequenceFileInputFormat.addInputPath(myJob, new Path(args[1]));
	String output=args[3];
	TextOutputFormat.setOutputPath(myJob, new Path(output));
	myJob.waitForCompletion(true);	
}
}


