# DisneyPlusETL

The code cleans Disney+ content data gathered from Kaggle, Wikipedia and IMDb.
Using Hadoop MapReduce, unwanted columns and incomplete rows are cleaned from the base dataset which is Disney+ data from Kaggle and Wikipedia. (https://www.kaggle.com/unanimad/disney-plus-shows, https://en.wikipedia.org/wiki/List_of_Disney%2B_original_films)
Profiling code counts the number of lines of a file, which can be used to find the difference between content data before and after cleaning.
Then, using Spark, the cleaned data is merged with IMDb data. (https://www.imdb.com/interfaces/)
