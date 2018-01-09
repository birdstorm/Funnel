### 使用方法：
1. 在机器安装[maven](maven.apache.org)
2. 打开cd到pom.xml所在目录
3. 执行`mvn clean package`
4. 生成好的jar包会放在`./target`目录下，分别为`yukuo.spark-1.0-SNAPSHOT-jar-with-dependencies.jar`和`yukuo.spark-1.0-SNAPSHOT.jar` (前者会自带程序所用到的依赖包，推荐使用前者)
5. cd到spark所在目录，配置好tispark环境，参考[这里](https://github.com/pingcap/tispark).
6. 执行spark-submit命令：
```bash
./bin/spark-submit --class yukuo.OLAPJobRunner /存放此jar的路径/yukuo.spark-1.0-SNAPSHOT-jar-with-dependencies.jar
```
7. 之后程序会开始运行，弹出以下提示
```bash
请输入日期(yyyyMMdd):
```

按照您的需求输入需要计算的开始时间，如：20171118


### 配置文件：
`src/main/resources/yukuo_io_config.properties`保存了一些可配置的选项：

|    Key    | 默认值 | 描述 |
| ---------- | --- | --- |
| yukuo.olap.job.output.path |  /home/tidb/olap/ | OLAP任务生成的csv文件存放路径 |
| yukuo.olap.job.dbName |  chiji_db | 需要映射的TiDB数据库名称 |
| yukuo.olap.job.output.name |  {任务开始执行的时间(yyyyMMdd_HHmmss)}_job_result_from_{输入的起始日期时间戳}.csv | 输出文件名 |

您可以根据实际情况进行自定义