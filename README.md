# BigData-Coursework
Coursework for the Big Data course at the University of Glasgow. Coursework was completed as part of a two-man team, please see assignment README.txt files for more information. Course taken in the 2017-2018 academic year.

---------------------------------------

The coursework consisted of 2 assignments:

* Assignment 1 = Implement a watered-down version of the PageRank algorithm in Hadoop MapReduce. Assignment 1 is located in the [mapreduce folder](mapreduce).
* Assignment 2 = Implement a watered-down version of the PageRank algorithm in Spark. Assignment 2 is located in the [spark folder](spark).

---------------------------------------

Both assignments used data from a dataset that consisted of Wikipedia edit history. 
The dataset is a single large text file that is approximately 300GB in size.
Each revision history record consists of 14 lines, each starting with a tag and containing a space- delimited series of entries. 
More specifically, each record contains the following data/tags, one tag per line:
* REVISION: revision metadata, consisting of:
     * article_id: a large integer, uniquely identifying each page.
     * rev_id: a large number uniquely identifying each revision.
     * article_title: a string denoting the page’s title (and the last part of the URL of the page).
     * timestamp: the exact date and time of the revision, in ISO 8601 format; e.g., 13:45:00 UTC 30 September 2013 becomes 2013-09-12T13:45:00Z, where T separates the date from the time part and Z denotes the time is in UTC.
     * [ip:]username: the name of the user who performed the revision, or her DNS-resolved IP address (e.g., ip:office.dcs.gla.ac.uk) if anonymous.
     * user_id: a large number uniquely identifying the user who performed the revision, or her IP address as above if anonymous.
* CATEGORY: list of categories this page is assigned to.
* IMAGE: list of images in the page, each listed as many times as it occurs.
* MAIN, TALK, USER, USER_TALK, OTHER: cross-references to pages in other namespaces.
* EXTERNAL: list of hyperlinks to pages outside Wikipedia.
* TEMPLATE: list of all templates used by the page, each listed as many times as it occurs.
* COMMENT: revision comments as entered by the revision author.
* MINOR: a Boolean flag (0|1) denoting whether the edit was marked as minor by the author.
* TEXTDATA: word count of revision's plain text.
* An empty line, denoting the end of the current record.

----------------------------------------

The goal of each assignment is to produce an output file of article_id and pagerank_score pairs in descending order, where the file contains of line of text for each unique article_id.
