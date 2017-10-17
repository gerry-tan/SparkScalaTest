/**
 * Created by Tan on 2017/8/14.
 */
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 通话记录表:
 * rowkey :  手机号+时间（天）
 *
 * 18684640986_20160909
 * 18684640986_20160910
 *
 * 修改  rowkey :  手机号+(99999999-时间（天）)
 *
 * @author root
 *
 */
public class HbaseTest {

    public static Random r =new Random();
    public static String T_N ="t_phone_cdr";
    public HBaseAdmin  admin =null;//ddl
    public HTable   htable =null;//dml
    public static  byte[] CF="cdr".getBytes();

    @Before
    public void before()throws Exception{
        Configuration conf =HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "spark001,spark002,spark003");
        admin =new HBaseAdmin(conf);
        htable =new HTable(conf, T_N);
    }

    @After
    public void end()throws Exception{
        if(admin!=null){
            admin.close();
        }
        if(htable!=null){
            htable.close();
        }
    }

    @Test
    public void createTable()throws Exception{
        HTableDescriptor table =new HTableDescriptor(TableName.valueOf(T_N));

        HColumnDescriptor cf =new HColumnDescriptor("cdr");
        cf.setMaxVersions(1);
        cf.setBlockCacheEnabled(true);

        table.addFamily(cf);

        if(admin.tableExists(T_N)){
            admin.disableTable(T_N);
            admin.deleteTable(T_N);
        }

        admin.createTable(table);
    }

    @Test
    public void insert()throws Exception{

        for(int i=0;i<1000;i++){
            List<Put> ps =new ArrayList<Put>();
            String phone =getPhone("186");
            for(int j=0 ;j<100;j++){
                String day =getDay("2017");
                String rowkey =phone+"_"+(Integer.MAX_VALUE-Integer.parseInt(day));
                Put p =new Put(rowkey.getBytes());
                p.add(CF, "source".getBytes(), phone.getBytes());
                p.add(CF, "dest".getBytes(), getPhone("139").getBytes());
                p.add(CF, "date".getBytes(), getDate(day).getBytes());
                p.add(CF, "type".getBytes(), (r.nextInt(2)+"").getBytes());//0 表示主叫，1 表示被叫
                ps.add(p);
            }
            htable.put(ps);
        }
    }

    @Test
    public void getOneRow()throws Exception{
        String rowkey ="18600256042_2127312422";
        Get get =new Get(rowkey.getBytes());
        Result res = htable.get(get);//result相当于一行数据
        System.out.println(new String(CellUtil.cloneValue(res.getColumnLatestCell(CF, "dest".getBytes()))));
        System.out.println(new String(CellUtil.cloneValue(res.getColumnLatestCell(CF, "date".getBytes()))));
        System.out.println(new String(CellUtil.cloneValue(res.getColumnLatestCell(CF, "source".getBytes()))));
    }

    /**
     * 1、18648350150 7月份的所有通话记录
     * 2、18648350150 所有的通话记录
     * 3、18648350150 所有的主叫目标号码，和通话时间
     */
    @Test
    public void find()throws Exception{
        Scan scan =new Scan();//整表逐行遍历，范围查询
        //1、
//		scan.setStartRow(("18648350150"+"_"+(Integer.MAX_VALUE-20160731)).getBytes());
//		scan.setStopRow(("18648350150"+"_"+(Integer.MAX_VALUE-20160701)).getBytes());

        //2、
//		PrefixFilter filter =new PrefixFilter("18648350150".getBytes());
//		scan.setFilter(filter);


        //3、

        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);//条件的组合对象
        PrefixFilter f1 =new PrefixFilter("18605781981".getBytes());
        SingleColumnValueFilter f2 =new SingleColumnValueFilter(CF, "type".getBytes(), CompareOp.EQUAL, "0".getBytes());
        list.addFilter(f1);
        list.addFilter(f2);

        scan.addColumn(CF, "dest".getBytes());//只返回给客户端dest和date两个列，其他列不会返回
        scan.addColumn(CF, "date".getBytes());
        scan.setFilter(list);


        ResultScanner ress = htable.getScanner(scan);//ResultScanner 多行数据
        for(Result res : ress){
            System.out.println(new String(CellUtil.cloneValue(res.getColumnLatestCell(CF, "dest".getBytes()))));
            System.out.println(new String(CellUtil.cloneValue(res.getColumnLatestCell(CF, "date".getBytes()))));
//			System.out.println(new String(CellUtil.cloneValue(res.getColumnLatestCell(CF, "type".getBytes()))));
        }
    }

    public void delete()throws Exception{
        Delete d =new Delete("18648350150_2127323033".getBytes());
        //删除一个单元格
        d.deleteColumn(CF, "type".getBytes());
        htable.delete(d);
        //删除7月份的数据，首先通过SCAN 查询出来rowkey，然后一次删除
    }

    public static String getPhone(String prefix){
        return prefix+String.format("%08d", r.nextInt(100000000));
    }

    public static String getDay(String year){
        return year+String.format("%02d%02d",new Object[]{ r.nextInt(12)+1,r.nextInt(30)+1});
    }

    public static String getDate(String day){
        return day+String.format("%02d%02d%02d",new Object[]{ r.nextInt(24),r.nextInt(60),r.nextInt(60)});
    }


}

