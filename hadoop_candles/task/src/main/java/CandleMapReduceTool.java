import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

public class CandleMapReduceTool extends Configured implements Tool {

    public static class CandleMapper
	extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//#SYMBOL,SYSTEM,MOMENT,ID_DEAL,PRICE_DEAL,VOLUME,OPEN_POS,DIRECTION
//SVH1,F,2011 01 11 10 00 00 080,255223067,30.46000,1,8714,S
            Configuration conf = context.getConfiguration();
            String regexp = conf.get("candle.securities");
            Integer width = Integer.valueOf(conf.get("candle.width"));
            Integer startdate = Integer.valueOf(conf.get("candle.date.from"));
            Integer enddate = Integer.valueOf(conf.get("candle.date.to"));
            Integer starttime = Integer.valueOf(conf.get("candle.time.from"));
            Integer endtime = Integer.valueOf(conf.get("candle.time.to"));
            Text keyres = new Text();
            Text valueres = new Text();

            String symbol;
            String[] tokens = value.toString().split(",");
            Integer volume, dateNum, time, hhmm;
            Long ms, moment, candleStart;
            Double price;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
            Date date;

            try {
                symbol = tokens[0];

                if (!Pattern.matches(regexp, symbol)) {
                    return;
                }
                moment = Long.valueOf(tokens[2]);
                date = sdf.parse(tokens[2]);
                price = Double.valueOf(tokens[4]);
                dateNum = (int)(moment / 1000000000);
                time = (int)(moment % 1000000000);
                hhmm = (int)(time / 100000);
                
                if ((dateNum < startdate) || (dateNum >= enddate)) {
                    return;
                }
                if ((hhmm < starttime) || (hhmm >= endtime)) {
                    return;
                }
                ms = date.getTime();
                candleStart = ms - ms % width;  

                Date res = new Date(candleStart);
                keyres.set(symbol + "," + String.valueOf(sdf.format(res)));
                valueres.set(String.valueOf(price) + "," + tokens[2] + "," + tokens[3]);

            } catch (Exception e) {
                System.err.println(e);
            }
            context.write(keyres, valueres);
	    }
    }
// /opt/hadoop-3.3.6/bin/hadoop jar ./target/candle-1.jar CandleMapReduceTool --conf ./config.xml ./src/main/java/input ./src/main/res
    public static class CandleReducer
	extends Reducer<Object, Text, NullWritable, Text> {
        private MultipleOutputs mos;
        public void setup(Context context) {
            mos = new MultipleOutputs(context);
        }

        public void reduce(Object key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] symbmoment = key.toString().split(",");
            if (symbmoment.length != 2) {
                return;
            }
            String symbol = symbmoment[0];
            String moment = symbmoment[1];

            double minprice = 0.0;
            double maxprice = 0.0;
            double openprice = 0.0;
            Long opentime = -1L;
            Long openid = -1L;

            double closeprice = 0.0;
            Long closetime = -1L;
            Long closeid = -1L;
            NullWritable keyres = NullWritable.get();
            Text valueres = new Text();
            // iterate through Iterable, parse each element, update min, max, open, close prices.
            for (Text item : values)
            {
                String[] pricetimeid = item.toString().split(",");
                Double price = Double.valueOf(pricetimeid[0]);
                Long time = Long.valueOf(pricetimeid[1]);
                Long id = Long.valueOf(pricetimeid[2]);
                if (minprice == 0.0) {
                    minprice = price;
                    maxprice = price;
                    openprice = price;
                    opentime = time;
                    openid = id;
                    closeprice = price;
                    closetime = time;
                    closeid = id;
                }
                if (price < minprice) {
                    minprice = price;
                } else if (price > maxprice) {
                    maxprice = price;
                }
                if (time < opentime) {
                    openprice = price;
                    opentime = time;
                    openid = id;
                } else if ((time == opentime) && (id < openid)) {
                    openprice = price;
                    opentime = time;
                    openid = id;
                } else if (time > closetime) {
                    closeprice = price;
                    closetime = time;
                    closeid = id;
                } else if ((time == closetime) && (id > closeid)) {
                    closeprice = price;
                    closetime = time;
                    closeid = id;
                }
            }
            keyres = NullWritable.get();

            valueres.set(symbol+","+moment+","+String.valueOf(openprice)+","+String.valueOf(maxprice)+","+String.valueOf(minprice)+","+String.valueOf(closeprice));
            // SYMBOL,MOMENT,OPEN,HIGH,LOW,CLOSE
            mos.write(keyres, valueres, symbol);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CandleMapReduceTool(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        System.out.println(conf.get("candle.securities"));
        if (conf.get("candle.securities") == null) {
            conf.set("candle.securities", ".*");
        }
        System.out.println(conf.get("candle.width"));
        if (conf.get("candle.width") == null) {
            conf.set("candle.width", "300000");
        }
        System.out.println(conf.get("candle.date.from"));
        if (conf.get("candle.date.from") == null) {
            conf.set("candle.date.from", "19000101");
        }
        System.out.println(conf.get("candle.date.to"));
        if (conf.get("candle.date.to") == null) {
            conf.set("candle.date.to", "20200101");
        }
        System.out.println(conf.get("candle.time.from"));
        if (conf.get("candle.time.from") == null) {
            conf.set("candle.time.from", "1000");
        }
        System.out.println(conf.get("candle.time.to"));
        if (conf.get("candle.time.to") == null) {
            conf.set("candle.time.to", "1800");
        }
        System.out.println(conf.get("candle.num.reducers"));
        if (conf.get("candle.num.reducers") == null) {
            conf.set("candle.num.reducers", "1");
        }

        String INPUT = args[0];
        String OUTPUT = args[1];
        Job job = Job.getInstance(conf, "Candles");
        job.setJarByClass(CandleMapReduceTool.class);
        job.setMapperClass(CandleMapper.class);
        // job.setCombinerClass(CandleReducer.class);
        job.setReducerClass(CandleReducer.class);
        job.setNumReduceTasks(Integer.valueOf(conf.get("candle.num.reducers")));
        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(INPUT));
        TextOutputFormat.setOutputPath(job, new Path(OUTPUT));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        return(job.waitForCompletion(true) ? 0 : 1);
    }
}
