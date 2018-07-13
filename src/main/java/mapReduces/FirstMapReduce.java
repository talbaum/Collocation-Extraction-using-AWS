package mapReduces;



import com.amazonaws.samples.Bigram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.StringTokenizer;

// This map reduce is filtering stop words (eng/heb),
// and calculating C(w1,w2) for each pair by decade.

public class FirstMapReduce {

	public FirstMapReduce() {}

	public static class FirstMapReduceMapper extends Mapper<LongWritable, Text, Bigram, LongWritable> {
		
		//try to send here the stopwords we got from main instead of this hard coded

		final String[] EnglishStopWords = {"don't","your","without","via","these","would","because","near","ten","unlikely","thus","meanwhile","viz","here�s","yourselves","contains","eleven","detail","much","appropriate","wasn�t","anybody","least","example","same",
				"after","a","thanx","namely","i","the","fifth","thank","yours","novel","nine","hasn�t","got","empty","wish","besides","serious","others","need","its","often","onto","gone","aside","therefore",
				"hardly","that�s","useful","downwards","c's","nowhere","sorry","provides","forty","better","with","there","well","happens","tries","tried","per","went","considering","nothing","anyhow","specify","forth","ever","system",
				"even","thats","hundred","other","indicated","against","respectively","a's","howbeit","top","too","indicates","have","accordingly","particularly","thoroughly","awfully","who�s","ain't","com","con","almost","amoungst","upon","latterly",
				"amongst","etc","whether","quite","all","always","new","took","already","below","everyone","didn't","lest","shall","less","were","try","became","cause","around","it�s","and","saying","says","fifteen",
				"it�d","whence","cry","any","despite","followed","until","formerly","shouldn�t","gotten","anywhere","wherein","let","welcome","using","containing","want","each","specifying","himself","must","wouldn�t","maybe","probably","another",
				"two","anyway","found","are","does","taken","came","where","gives","think","entirely","call","such","doesn�t","ask","describe","through","anyways","becoming","comes, 'concerning","cant","had","weren�t","either","ours",
				"yourself","has","those","seeming","given","last","might","whatever","everywhere","name","overall","full","next","away","asking","nearly","show","you�re","non","anything","nor","not","now","hence","unto",
				"yes","was","yet","way","can�t","inasmuch","what","furthermore","hadn�t","three","when","put","her","whoever","far","truly","okay","give","having","hereupon","noone","couldnt","computer","merely","more",
				"unfortunately","lately","certain","before","tell","used","him","looks","his","few","consider","keeps","you�ll","described","otherwise","whither","particular","done","inner","both","most","twice","outside","keep","who",
				"part","their","why","elsewhere","alone","along","ltd","amount","move","hereafter","saw","also","say","enough","gets","someone","third","mean","various","neither","latter","uses","front","further","sometime",
				"been","mostly","hasnt","couldn't","appreciate","doesn't","you","afterwards","sure","going","bill","am","an","whose","former","mill","as","at","trying","they�ll","it�ll","looking","be","consequently","they�d",
				"how","see","inward","won�t","by","whom","indicate","mine","sixty","contain","possible","right","co","somewhat","under","did","de","sometimes","do","down","later","which","needs","ignored","eg",
				"thereafter","regarding","et","never","she","take","ex","immediate","relatively","aren't","little","however","some","rather","for","back","greetings","getting","perhaps","just","over","six","thence","where�s","go",
				"obviously","kept","they�re","let�s","although","selves","fify","he","isn�t","very","hi","he�s","placed","therein","thick","soon","thanks","else","four","beside","whereas","usually","ie","if","likely",
				"in","made","is","it","being","you�d","somebody","hello","whereby","secondly","become","whereupon","we�re","you�ve","eight","known","theres","hopefully","everything","can't","together","twenty","knows","side","may",
				"seemed","within","could","they�ve","off","able","theirs","presumably","use","several","while","liked","second","that","i�d","find","than","me","i�m","different","insofar","regardless","follows","seriously","fill",
				"my","plus","becomes","nd","couldn�t","since","behind","no","what�s","we�ve","best","of","hither","oh","somehow","ok","on","allows","brief","certainly","or","exactly","c'mon","due","about",
				"somewhere","above","fire","they","qv","old","myself","herein","them","then","something","rd","re","thereby","twelve","except","sincere","sub","nevertheless","don�t","believe","seen","seem","sup","into",
				"unless","so","apart","ought","though","necessary","one","aren�t","thorough","many","actually","appear","definitely","th","associated","to","but","we�d","willing","available","seven","mainly","zero","whenever","un",
				"up","five","us","beforehand","this","please","reasonably","look","thin","especially","once","know","vs","allow","que","doing","changes","ain�t","we","interest","themselves","throughout","wants","wonder","every",
				"again","t�s","indeed","i�ll","we�ll","ones","whole","during","none","beyond","didn�t","c�s","nobody","between","still","come","itself","toward","among","anyone","following","c�mon","our","ourselves","there�s",
				"specified","out","across","seeing","moreover","get","causes","course","sensible","wherever","help","cannot","self","hereby","whereafter","first","thru","own","clearly","should","only","from","a�s","like","bottom",
				"goes","towards","regards","sent","edu","herself","seems","thereupon","here","haven�t","everybody","according","hers","can","i�ve","said","value","inc","will","instead","really","currently","corresponding","tends","normally"};

		final String[] HebrewStopWords = {"��", "��", "��", "��", "����", "�����", "��", "��", "��", "���", "��", "���", "��"
				, "�", "���", "���", "��", "��", "�����", "��", "��", "��", "���", "���", "��", "��", "��", "����", "����",
				"��", "�", "��", "�", "���", "���", "���", "���", "���", "����", "����", "���", "�", "����", "���", "���",
				"���", "���", "���", "���", "���", "�", "�", "���", "���", "��", "��", "��", "��", "���", "��", "���", "��",
				"��", "���", "���", "���", "���", "���", "��", "����", "�", "^", "?", ";", ":", "1", ".", "-", "*", "\"",
				"!", "����", "���", "���", ")", "����", "��", "���", "����", "����", "�����", "���", "���", "�����", "����",
				"���", "�����", "����", "������", "����", "����", "����", "����", "�����", "���", "����", "����", "����",
				"����", "���", "(", "���", "���", "���", "���", "�", "�", "�����", "����", "�", "����", "���", "���", "�",
				"���", "�", "���", "���", "�", "��", "���", "����", "����", "����", "��", "����", "��", "�", "��", "���",
				"���", "�", "��", "��", "����", "��", "���", "�"};

		public FirstMapReduceMapper() {
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			// was a variable: is stop word included which is not the case in our work - not needed?
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws NumberFormatException,IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());

			if(itr.countTokens() == 6) { //if it is our format - firstword secondword year occurences pages books
				String first_string = itr.nextToken();
				if (first_string.contains("_")) first_string = first_string.substring(0, first_string.indexOf("_"));
					Text first = new Text(first_string);

				String second_string = itr.nextToken();
				if (second_string.contains("_")) second_string = second_string.substring(0, second_string.indexOf("_"));
					Text second = new Text(second_string);
				
				boolean exist = false;
				if (context.getConfiguration().get("language").equals("eng")) {
					first_string = first_string.trim().toLowerCase();
					second_string = second_string.trim().toLowerCase();
					exist=doesStopWordExist(EnglishStopWords,first_string,second_string);
					if (!exist) 
						writeToContext(context,itr,first,second); 
				}
				else {
					exist=doesStopWordExist(HebrewStopWords,first_string,second_string);
					if (!exist) 
						writeToContext(context,itr,first,second); 
				}
			}
		}


		private static boolean doesStopWordExist(String[] StopWords, String first_string, String second_string) {
			for (int i = 0; i < StopWords.length; i++) {
				if (StopWords[i].equals(first_string) || StopWords[i].equals(second_string) || first_string.equals("") || second_string.equals("")) 
					return true;
			}
			return false;
		}
		
		private static void writeToContext(Context context, StringTokenizer itr, Text first, Text second) throws NumberFormatException, IOException, InterruptedException {
			Text year = new Text(itr.nextToken().substring(0, 3));
			Text decade = getDecade(year);
			Text numberOfOccurrences = new Text(itr.nextToken());
			Bigram bigram = new Bigram(first, second, decade);
			context.write(bigram, new LongWritable(Integer.parseInt(numberOfOccurrences.toString())));
		}


		private static Text getDecade(Text yearStr) {
			int yearInt=Integer.parseInt(yearStr.toString());
			int decade= (yearInt / 10) * 10;
			Text ans =new Text(String.valueOf(decade));
			return ans;
		}


		public static class FirstMapReduceReducer extends Reducer<Bigram,LongWritable,Bigram,LongWritable> {
			@Override
			public void reduce(Bigram key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
				long sum = 0;
				for (LongWritable value : values) {
					sum += value.get();
				}

				context.write(key, new LongWritable(sum));
			}
		}
	
	}
}

