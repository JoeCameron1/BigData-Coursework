Big Data Assignment 1 - Hadoop MapReduce PageRank

Group Members: Joseph Cameron (2117625c) and Hamish Watters (2131974w)

------------------------------------------------------------
ASSUMPTIONS

Regarding the input, we are assuming that it will always be in the form of a text file, where each article contains at least a REVISION section which contains the article's title as the fourth token in the line, and a MAIN section which contains all content (including links) from the article.

------------------------------------------------------------
DESIGN AND JUSTIFICATION

Our PageRank implementation has been split into three individual MapReduce jobs.
Each mapper and reducer class is contained in a separate file to promote good software engineering practices such as modularity.

Corresponding Java Files:
PageRank.java - The main PageRank file where every job is scheduled.
TransformInputMap.java - The mapper and reducer for the first job
TransformInputReduce.java
CalculatePageRankMap.java - The mapper and reducer for the second job
CalculatePageRankReduce.java
ProduceOutputMap.java - The mapper and reducer for the third job.
ProduceOutputReduce.java
All other files are from practice tasks.

The first job extracts article titles from lines beginning REVISION, and external links for that article from lines beginning MAIN using its mapper. The reducer then generates an output with the article title as key, and a text value of “1.0 (starting page rank) <external link a> <external link b> ...”.
The mapper for the second job then sums for each value, the page rank divided by the number of external links, and uses this in the equation 0.85 * Sum + 0.15 to calculate the new page rank for that page. The reducer then generates an identical output to the first job, to allow page rank to be calculated iteratively.
Finally, the last job formats the output with keys of article name and values of the page rank score, in a descending order based on the page rank.

------------------------------------------------------------
INTERESTING ASPECTS

Overall, there are many interesting aspects to our solution.
Firstly, we represent page rank iterations by running the second job multiple times.
Secondly, we make frequent use of well-known and optimised Java libraries such as StringTokenizer (instead of String.split()) and StringBuilder (instead of string concatenation).
We found that these constructs gave a slightly quicker runtime and improved performance, which we consider to be extremely important when dealing with big data.
