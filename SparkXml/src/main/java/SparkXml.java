import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import java.util.*;
import java.util.regex.*;
import java.io.*;


public class SparkXml {
    private static long MinorCount(Dataset<Row> df) {
        Dataset<Row> dfMinorInRevision=df.select("revision.minor").
                filter("revision.minor is not null");
        return dfMinorInRevision.count();
    }

    private static void outputMinorCount
            (Dataset<Row> df, BufferedWriter writer) throws Exception {
        writer.write(MinorCount(df)+"\n");
    }

    private static Dataset<Row> pagesAtMost5Url(Dataset<Row> df) {
        Dataset<Row> idTitleRevSet=df.select("id", "title", "revision.text._VALUE").
                 filter("revision.text._VALUE is not null");
        return idTitleRevSet.filter((FilterFunction<Row>) r->{
            String text=r.getString(2);
            Pattern urlPattern=Pattern.compile("https?://", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
            Matcher matcher=urlPattern.matcher(text);
            int count=0;
            while(matcher.find()) {
                count++;
            }
            return count<=5;
        });
    }

    private static void outputPagesAtMost5Url
            (Dataset<Row> df, String path) {
        df.javaRDD().map(row->String.format("%-20d%-50s", row.getLong(0), row.getString(1))).repartition(1).saveAsTextFile(path);
    }

    private static Dataset<Row> getContributors(Dataset<Row> df, SparkSession sparkSession) throws Exception {
        Dataset<Row> contributorSet=df.select(df.col("revision.contributor.id").as("contributor_id"),
                df.col("revision.contributor.username").as("contributor_username"),
                df.col("revision.id").as("revision_id"),
                df.col("revision.timestamp").as("revision_timestamp")).filter("contributor_id is not null").filter("revision_id is not null");
        contributorSet.createGlobalTempView("temp");
        Dataset<Row> contributorWithMore1RevisionSet=sparkSession.sql("select contributor_id as contributor_id_temp, count(revision_id) as revision_num " +
                "from global_temp.temp group by contributor_id having revision_num>1");
        sparkSession.catalog().dropGlobalTempView("temp");
        Dataset<Row> resultContributorSet=contributorWithMore1RevisionSet.
                join(contributorSet,
                        contributorWithMore1RevisionSet.col("contributor_id_temp").
                                equalTo(contributorSet.col("contributor_id"))).
                orderBy(col("revision_timestamp").desc()).select("contributor_id", "contributor_username", "revision_id", "revision_timestamp");
        Dataset<Row> result=resultContributorSet.groupBy("contributor_id", "contributor_username").
                agg(functions.collect_list(col("revision_id")).as("revision_id_list")).
                orderBy("contributor_id").select("contributor_id", "contributor_username", "revision_id_list");
        return result;
    }

    private static void outputContributors(Dataset<Row> df, String path) {
//        df.javaRDD().map(row->{
//            String format="%-20d%-40s"+"%-"+row.getString(2).length()+"s";
//            return String.format(format, row.getLong(0), row.getString(1), row.getString(2));}).repartition(1).saveAsTextFile(path);
        df.javaRDD().repartition(1).saveAsTextFile(path);
    }

    public static void main(String[] args) throws Exception {
        long startTime=System.currentTimeMillis();
        BufferedWriter writer=new BufferedWriter(new FileWriter(args[1]+"time.txt"));
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkXml");
        sparkConf.set("spark.driver.memory", "8g");
        sparkConf.set("spark.memory.offHeap.enabled","true");
        sparkConf.set("spark.memory.offHeap.size","13g");
        sparkConf.set("spark.sql.shuffle.partitions", "30");
        //sparkConf.set("spark.default.parallelism", "100");
        SparkContext sparkContext = new SparkContext(sparkConf);
        SparkSession sparkSession= new SparkSession(sparkContext);

        Dataset<Row> dataframe = sparkSession.read().format("com.databricks.spark.xml").
                option("rowTag", "page").load(args[0]);
        dataframe.persist();
        //outputMinorCount(dataframe, writer);
        outputPagesAtMost5Url(pagesAtMost5Url(dataframe), args[1]+"output");
        //outputContributors(getContributors(dataframe, sparkSession), args[1]+"output");
        //getContributors(dataframe, sparkSession);
        long endTime=System.currentTimeMillis();
        writer.write("\nTotal Execution Time is: "+(endTime-startTime)/1000+" s\n");
        writer.close();
    }
}
