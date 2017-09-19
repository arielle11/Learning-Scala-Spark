

// select or extract a column (ColumnToExtract) by name from a 
// dataframe (df) and assign to new variable
// in R: newVariable <- df[,"ColumnToExtract"]
val newVariable = df.select("ColumnToExtract")

// Select multiple columns (Features2Select) 
// by name from a dataframe (df)
// then create subset df (newDf) with just those columns
// in R: Features2Select <- c("ColName1". "ColName2") 
// newDF <- df[, Features2Select]
import org.apache.spark.sql.functions._
val Features2Select = Seq("ColName1", "ColName2")
val colNames = Features2Select.map(name => col(name))
val newDf = df.select(colNames: _*)

// print head of a dataframe (df)
df.show

// create a dataframe
val df = spark.createDataFrame(Seq(
  (0, "a", "t"),
  (1, "b", "t"),
  (2, "c", "t"),
  (3, "a", "j"),
  (4, "a", "j"),
  (5, "c", "j")
)).toDF("ColName1", "ColName2", "ColName3")

// paste a suffix or prefix onto a vector or list of strings
// in R: list_with_suffixes <- paste(list_of_strings, "suffix", sep = "")
val list_with_suffixes = list_of_strings map {_ ++ "suffix"}
val list_with_prefixes = list_of_strings map {"prefix" ++ _}

