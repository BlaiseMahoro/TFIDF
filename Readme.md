TFIDF: Term Fequency-Inverse Document Frequency
Good Algorithm to demonstrate chained MapReduce jobs
This algorithm is used in natural language processing for larger
algorithms like topic modeling and to determine the relevance of a
term to a particular document.
It is also extremely common in social network analysis, web analysis,
and other analyses where there is unstructured or raw language data.
Compute the Term Frequency-Inverse Document Frequency for words in thedataset of reuters.txt. The number of document N =| D |= 10788. We will filter out some common English words in this homework. Your mapper should not emit any of of the word found in stopwords.txt. Sort the final output by document id.
This app uses 3 hadoop jobs. 
- Job1:term frequency per document
- Job2:count how many document has the term
- Job3:compute tf*idf
     