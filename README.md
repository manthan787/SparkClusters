# A7: Clustering

### Running The Project

**Requirements**

Install `Java 1.8`

Download binaries for `Scala 2.11.8`

Download `Spark 2.2.0`

**R Requirements**

Install following dependencies to avoid errors while generating the Rmarkdown Report.

From your R console execute following commands:

```
install.packages("ggplot2")
install.packages("tm")
install.packages("wordcloud")
install.packages("formattable")
```

### Makefile

Before you build the project, make sure you've pointed `SCALA_HOME` to your Scala 2.11.8 binaries home directory and `SPARK_HOME` to Spark 2.2.0 home directory.

To build the project:  

    make build

To run all the variants:  

    make run

If you'd like to change the input path or output path, run it like so:

    make run INPUT_PATH=input/all/ OUTPUT_PATH=out/

To generate the report:

    make report
