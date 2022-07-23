Steps:

Setup:
1) Create a JAVA project in eclipse.
2) Add provided classes to the project.
3) Add external jars(hadoop, opennlp library) to the project.
4) Add PATH environment variable entry for opennlp library.
5) Create external jar of the project.

Execution:
1) Create two directories using hadoop fs as below:
	hadoop fs -mkdir ./DFInput/
	hadoop fs -mkdir ./TFInput/
	
2) Add Wikipedia files to the above mentioned directories
	hadoop fs -put <path-to-wikipedia-files> ./DFInput/
	hadoop fs -put <path-to-wikipedia-files> ./TFInput/

3) Copy project jar to current folder.

4) To calculate Document Frquency
	hadoop jar <jar-file-name> DF ./DFInput ./DFInput/output

5) To TFIDF Score
	hadoop har <jar-file-name> Score ./TFInput/ ./TFInput/output ./DFInput/output/part-r-00000
	
