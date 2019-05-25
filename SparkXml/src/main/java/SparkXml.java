import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import java.util.*;
import java.util.regex.*;
import java.io.*;


public class SparkXml {
    private static class Contributor {
        Long id;
        String username;
        Contributor(Long _id, String _username) {
            id=_id;
            username=_username;
        }
        @Override
        public boolean equals(Object ob) {
            if(!(ob instanceof Contributor)) {
                return false;
            }
            Contributor another=(Contributor)ob;
            return id.equals(another.id);
        }
        @Override
        public int hashCode() {
            return (int)((31*id+username.hashCode())%(long)Integer.MAX_VALUE);
        }
    }

    private static class Revision {
        Long id;
        String timestamp;
        Revision(Long _id, String _timestamp) {
            id=_id;
            timestamp=_timestamp;
        }
        @Override
        public boolean equals(Object ob) {
            if(!(ob instanceof Revision)) {
                return false;
            }
            Revision another=(Revision)ob;
            return id.equals(another.id);
        }
        @Override
        public int hashCode() {
            return (int)((31*id+timestamp.hashCode())%(long)Integer.MAX_VALUE);
        }
    }

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
            Pattern urlPattern=Pattern.compile("url=", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
            Matcher matcher=urlPattern.matcher(text);
            int count=0;
            while(matcher.find()) {
                count++;
            }
            return count<=5;
        });
    }

    private static void outputPagesAtMost5Url
            (Dataset<Row> df, String path) throws Exception {
        df.javaRDD().map(row->String.format("%-20d%-50s", row.getLong(0), row.getString(1))).repartition(1).saveAsTextFile(path);

//        df.foreach((ForeachFunction<Row>) row -> {
//            writer.write("111\n");
//        });

//        List<Row> rows=df.collectAsList();
//        int count=0;
//        writer.write(String.format("%-10s%-20s%-50s", "Index", "Id", "Title")+"\n");
//        for(Row row:rows) {
//            writer.write((String.format("%-10d%-20d%-50s", count++, row.getLong(0), row.getString(1)))+"\n");
//        }
    }

    private static TreeMap<Contributor, TreeSet<Revision>> getContributors(Dataset<Row> df) {
        Dataset<Row> contributorSet=df.select(df.col("revision.contributor.id").as("contributor_id"),
                df.col("revision.contributor.username").as("contributor_username"),
                df.col("revision.id").as("revision_id"),
                df.col("revision.timestamp").as("revision_timestamp")).filter("contributor_id is not null").filter("revision_id is not null");
        List<Row> rows=contributorSet.collectAsList();
        TreeMap<Contributor, TreeSet<Revision>> contributorToRevisions=new TreeMap<>((o1, o2)->Long.compare(o1.id, o2.id));

        for(Row row:rows) {
            Contributor contributor=new Contributor(row.getLong(0), row.getString(1));
            if(!contributorToRevisions.containsKey(contributor)) {
                contributorToRevisions.put(contributor, new TreeSet<>((o1, o2)->o2.timestamp.compareTo(o1.timestamp)));
            }
            contributorToRevisions.get(contributor).add(new Revision(row.getLong(2), row.getString(3)));
        }
        return contributorToRevisions;
    }

    private static void outputContributors
            (TreeMap<Contributor, TreeSet<Revision>> contributorToRevisions, BufferedWriter writer) throws Exception {
        for(Map.Entry<Contributor, TreeSet<Revision>> entry:contributorToRevisions.entrySet()) {
            Contributor contributor=entry.getKey();
            TreeSet<Revision> revisions=entry.getValue();
            if(revisions.size()>1) {
                writer.write(String.format("%-20d%-20s", contributor.id, contributor.username)+"\n");
                for(Revision revision:revisions) {
                    writer.write(String.format("%-20d%-20s", revision.id, revision.timestamp)+"\n");
                }
                writer.write("\n");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime=System.currentTimeMillis();
        BufferedWriter writer=new BufferedWriter(new FileWriter(args[1]+"output.txt"));
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
        //outputContributors(getContributors(dataframe), writer);
        long endTime=System.currentTimeMillis();
        writer.write("\nTotal Execution Time is: "+(endTime-startTime)/1000+" s\n");
        writer.close();
    }
}
