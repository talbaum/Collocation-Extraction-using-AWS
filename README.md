# Collocation-Extraction-using-AWS
In this project we have implemented  emneted a real-world application that automatically
extract collocations from the Google 2-grams dataset using Amazon Elastic Map Reduce.

## Team
We are two students and we implemented the project together.
Our names:

Tal Baum - btal - 204002828

Stas Radchenko - radchens - 319516985

## Instructions 
There are a few steps we have to follow in order to run the application smoothly.
First ,we have to upload the following jar files to our bucket:
step1.jar , step2.jar, step3.jar, step4.jar, step5.jar.

## How to run the project:

Now, we need to run this line in the terminal with the desired language( lang can be either eng or heb).  Get into the directory containing the main jar and run this command:
```
java -jar ExtractCollations.jar lang
```
## How the program works:
We will explain each step functionality at the map reduce system.

### Main
The main includes all the steps and the arguments that we need.
We create a cluster , give it all the steps and arguments,configure the path for saving the output for each step,as well the configuration to define the order in which evry step should run, and we let the application to run.

### Step1
Step1 is calculating C(w1,w2)- the number of appreances of the first word and second word toghether for each pair.
Step1 takes as input the 2-gram corpus given in the input, and parses it line by line. 
For each line from the 2-gram corpus ,after the map checks that neither word1 or word2 are stop words (pre defined) it creates a line with the word1 and word2.calculate the decade by year, that the combination of the words appeared ,and the number of their occurences in the corpus and sends it to the reducer.
In the reducer , we combine all the occurences by key.For each key we sum all the occurences.

### Step2
Step2 is calculating C(w1)- the number of appreances of the first word.
Step2 takes as input the output of Step1(From Step1 we recieve the number occurence of the combination of the words w1 with w2 in the whole corpus),and splits the first word w1 from the pair so that the reducer would calculate the number of occurences of this word.
In the reducer, we calculate the number of occurences of w1 * (* means all the words in the corpus) in the corpus and combine the answer with the calculation made in Step1.

### Step3
Step2 is calculating C(w2)- the number of appreances of the second word.
Step3 takes as input Step2 takes as input the output of Step2,and have a similiar logic as Step2, but this time it applied to the second word w2. For calculation of number of occurences w2, the map function append the w2 with *.
In reduce function as in Step2, appends the answer to already calculated steps 1,2.

### Step4
Step4 calculates the whole equation parts, including N (all the words in the corpus) and the desired log likehood parameter.
Step4 recieves as input the output of Step3.
The map function creates for the reduce a new type of <* ,*> pair, means that all the possible pairs in the corpus(the N parameter), so that the reduce function can calculate the N. 
In the reducer , we run over the whole corpus and calculate the N, afterwards we extract the calculations of the c(w1w2),c(w1),c(w2) from the previous steps, and calculate the likehood according to the equation:
<p>
  <img src="https://github.com/talbaum/Collocation-Extraction-using-AWS/blob/master/likehoodratio.JPG?raw=true"/>
</p>


### Step5
Step5 responsible to sort the words according to decade and likehood, and to pick the top 100 from each decade.
Step5 takes as input Step4 output.
The map function creates new type of data, that sorts each element by decade,if two elements have the same decade we sort them by likehood ratio.
In reduce function, we picked from each decade the top 100 entries (because we sorted them in descending order at the map function,) and write them in the output.


