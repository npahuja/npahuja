# Databricks notebook source
# MAGIC %md
# MAGIC ###Case study - Read data from json file and perform data curation

# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
import requests

spark = SparkSession.builder.appName("Word Count").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC Use Case - Read json file and check the row count 

df_inputFile = spark.read.json("s3a://csparkdata/ol_cdump.json")
print(df_inputFile.count() "number of rows are present in the dataset")
df_inputFile.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC * Data Profiling for Exploratory Data Analysis 

from pandas_profiling import ProfileReport as PR
df_origData_pd = df_inputFile.select("*").toPandas()
prof = PR(df_origData_pd.sample(n=1000))
prof.to_file(output_file='DataProfiling.html')

# COMMAND ----------

# MAGIC %md
# MAGIC - Clean the dataset 
# don't include in results with empty/null "titles" 
# "number of pages" should be greater than 20 
# "publishing year" is after 1950

print("Applying filters to create final dataset")

df_clean_data = df_origData.filter((df_origData.title.isNotNull())
                         & (df_origData.number_of_pages>20)
                         & (df_origData.title.publish_date>1950)
                         )

df_clean_data.createOrReplaceTempView("Cleaned_Data")
print("Total rows in final dataset are ",df_clean_data.count())

# COMMAND ----------

# MAGIC %md
# MAGIC - Requirment 4.a
# MAGIC - Get the first book / author which was published - and the last one. 

most_pages=df_clean_data.sort(col("number_of_pages").desc()).first()

print("Book with most pages is: ", most_pages.title, ", Number of pages: ", most_pages.number_of_pages)

sqldf_most_pages=spark.sql("Select title,number_of_pages from Cleaned_Data \
          where number_of_pages in (select max(number_of_pages) from Cleaned_Data )")

display(sqldf_most_pages)

# COMMAND ----------

# MAGIC %md
# MAGIC - Requirment 4.b
# MAGIC - Find the top 5 genres with most books.

# COMMAND ----------

topFiveGenres_df = df_clean_data.groupby("genres") \
                            .agg(count("genres").alias("count_of_books")) \
                            .sort(col("count_of_books").desc()) \
                            .filter("genres is not null") \
                            .limit(5)

print("Top five genres with most books : ")

display(topFiveGenres_df)
sql_topFiveGenres=spark.sql("select genres,count_of_books from ( \
          Select distinct genres,count(genres) count_of_books \
             ,dense_rank() over (order by count(genres) DESC ) dn \
             from Cleaned_Data where genres is not null group by genres) where dn<6")

sql_topFiveGenres.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC - Requirment 4.c
# MAGIC - Retrieve the top 5 authors who (co-)authored the most books.

# COMMAND ----------

df_authorKey=df_clean_data.withColumn('authors_keys', col("authors.key").getItem(0)) 
df_author=df_authorKey.withColumn('authors',split(col("authors_keys"), "/").getItem(2)) 

top_5_authors_df=df_author.groupBy('authors') \
                                    .agg(countDistinct('title').alias("authors_per_book")) \
                                    .filter((col("authors_per_book") > 1) & (col("authors").isNotNull()) ) \
                                    .sort(col("authors_per_book").desc())\
                                    .select("authors","authors_per_book").limit(5)

print("Top 5 authors who co-authored the most books : ")
display(top_5_authors_df)

top_5_authors_sqldf=spark.sql("select authors,authors_per_book from ( \
          select authors.key authors ,count(distinct(title)) authors_per_book, \
          dense_rank() over (order by count(title) desc ) dn \
          from Cleaned_Data where authors.key is not null \
              group by 1) where dn<6") 

top_5_authors_sqldf.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC - Requirment 4.d
# MAGIC - Per publish year, get the number of authors that published at least one book.

# COMMAND ----------

authors_publishyear_df=df_clean_data.groupby("publish_date","authors.key") \
                                        .agg(countDistinct("title").alias("count_of_books")) \
                                        .filter(col("count_of_books") >= 1) \
                                        .groupby("publish_date") \
                                        .agg(countDistinct("key").alias("count_of_authors")) \
                                        .sort("publish_date")

print("Per publish year- Count of authors that published atleast one book is ")
display(authors_publishyear_df)

authors_publishyear_sqldf=spark.sql("select publish_date publish_year,count(authors) no_of_authors from ( \
                                          select publish_date,authors.key authors , count(distinct(title)) no_of_tilte \
                                          from Cleaned_Data \
                                          group by 1,2 having count(title)>=1 ) \
                                     group by publish_date order by publish_date")
authors_publishyear_sqldf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC - Requirment 4.e
# MAGIC - Find the number of authors and number of books published per month for years between 1950 and 1970

# COMMAND ----------

authors_books_publishyr_df=df_clean_data.filter(col("publish_date") < 1980).groupby("publish_date")\
                                                        .agg(countDistinct("authors.key").alias("no_of_authors"),countDistinct("title").alias("no_of_books"))\
                                                        .sort("publish_date") \
                                                        .select("publish_date","no_of_authors","no_of_books",current_timestamp().alias("created_date"),current_timestamp().alias("modified_date"))

print("Number of authors and number of books published per year between 1950 to 1970 is : ")

display(authors_books_publishyr_df)

authors_books_publishyr_sqldf=spark.sql("select publish_date,count(distinct(authors.key)) no_of_authors,count(distinct(title)) no_of_books\
          from Cleaned_Data where publish_date<1970 \
              group by 1 order by publish_date") 

authors_books_publishyr_sqldf.show(20,False)


authors_books_publishyr_df.createOrReplaceTempView("authors_books_publishyr_daily")


# COMMAND ----------

# MAGIC %md
# MAGIC # DATA LOAD AND MERGE LOGIC
# MAGIC  - Requirment 5

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/casestudyadidatabricks/preproccessed/

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ADI_DB
# MAGIC Location '/mnt/casestudyadidatabricks/preproccessed/demo'

# COMMAND ----------

sqldf_most_pages.write.format("delta").mode("overwrite").saveAsTable("ADI_DB.most_pages")
topFiveGenres_df.write.format("delta").mode("overwrite").saveAsTable("ADI_DB.topfivegenres")
top_5_authors_df.write.format("delta").mode("overwrite").saveAsTable("ADI_DB.top_5_authors")
authors_publishyear_df.write.format("delta").mode("overwrite").saveAsTable("ADI_DB.authors_publishyear")
authors_books_publishyr_df.write.format("delta").mode("overwrite").saveAsTable("ADI_DB.authors_books_publishyr")

# COMMAND ----------

authors_books_publishyr_df.createOrReplaceTempView("authors_books_publishyr_daily")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from ADI_DB.authors_books_publishyr

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ADI_DB.authors_books_publishyr tgt
# MAGIC USING authors_books_publishyr_daily src
# MAGIC ON tgt.publish_date = src.publish_date
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.no_of_authors = src.no_of_authors
# MAGIC              ,tgt.no_of_books = src.no_of_books
# MAGIC              ,tgt.modified_date = src.modified_date
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (publish_date, no_of_authors, no_of_books, created_date,modified_date) VALUES (publish_date, no_of_authors, no_of_books, current_timestamp,current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from ADI_DB.authors_books_publishyr
