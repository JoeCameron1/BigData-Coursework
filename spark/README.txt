Big Data Assignment 2 - Spark PageRank

Group Members: Joseph Cameron (2117625c) and Hamish Watters (2131974w)

------------------------------------------------------------
ASSUMPTIONS

Regarding the input, we are assuming that it will always be in the form of a text file, where each article contains at least a line beginning "REVISION" which contains the article's title as the fourth token in the line and it's ISO8061 formatted timestamp as the fifth token, and a line beginning "MAIN" which contains all content (including links) from the article.

------------------------------------------------------------
DESIGN AND JUSTIFICATION

All relevant code in our PageRank implementation is contained within the src.main.java.spark.PageRank.java file.

When the text file is read, it is delimited by two new lines as opposed to one, as the individual records in the input file are separated as such. Next, the record is split into lines, each line for a section. Article title and timestamp are pulled out of the line beginning REVISION, while all the tokens in the line beginning MAIN become the outlinks for the record.
This output is then stored in a JavaPairRDD<String, Tuple<Long, Iterable<String>>. The timestamp is stored as a value in order to preserve the unique key. When extracting the list of outlinks, duplicate links are removed from the list by converting it to a stream, and running the distinct method.
In order to ensure only the latest revision is used, a reduceByKey is implemented which compares the timestamp of the two values, and returns the value with the maximum timestamp. The end result of this is returning the value with the maximum timestamp for each article title.
The remainder of the design is based on the PageRank implementation seen in the lecture slides.
