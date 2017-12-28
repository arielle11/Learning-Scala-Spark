

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

// add a string (NewElement) to an array of strings (ArrayOfStrings)
// append an element to array
// in R: c(ArrayOfStrings, "NewElement")
ArrayOfStrings :+ "NewElement"

// simple join of two dfs (df1 and df2) when columns to join on have same name (ColName)
// in R: merge(df1, df2, by = "ColName")
df1.join(df2, Seq("ColName"))

// get distinct elements from an array (Arr)
// in R: unique(Arr)
arr.distinct

// get a count of distinct elements
// in R: length(unique(Arr))
countDistinct(Arr) ; approx_count_distinct(Arr)

// get a count of distinct elements for all columns in a dataframe (df)
// in R: apply(X = df,  MARGIN = 2, FUN = function(x){length(unique(x))}
// can also use countDistinct but approx_count_distinct uses HLL and is faster.
df.select(df.columns.map(c => approx_count_distinct(col(c)).alias(c)): _*)

// get structure of a df
df.printSchema()

// function to drop columns from a dataframe (df). Input is an array of column names (Arr)
 def dropEm( columnsToDrop: Array[String], df: org.apache.spark.sql.DataFrame) : org.apache.spark.sql.DataFrame  = {
      var cols2drop = columnsToDrop
      var cols2keep = df.columns.diff(cols2drop)
      var colNames2keep = cols2keep.map(name => col(name))
      var sub = df.select(colNames2keep:_*)
      return sub
   }
dropEm(Arr, df)

// drop a list of columns names (features2Drop) from a dataframe(df) using foldLeft
val features2Drop = List("a", "b")
val subDF = features2Drop.foldLeft(df) { (df, s) => df.drop(s) }

// check to see if there is a column in a dataframe
import scala.util.Try
import org.apache.spark.sql.DataFrame
def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
