# 0 Background

## 0.1 通用Lambda架构

### 根据现有大数据项目重点进行整合

![image-20210422142737611](https://i.loli.net/2021/04/22/2Yc1IgWeU3bhpui.png)



### 集成架构

![image-20210422141241173](https://i.loli.net/2021/04/22/r24WzYqCZVsFclk.png)



## 0.2 环境准备与基础信息

- **世纪互联网址:** https://portal.azure.cn/#home
- **账号：**
- **密码：**
- **CDH中HBase地址为：**
- **databricks工作区网址：**
- **ADLS：**

# 1 连通性准备

## 1.1 Databricks连接ADLS

### 错误方法：用key vault的scope连接方式

- **这个API被改了，只能通过直接连接的方式**

- 原本的代码：

```scala
val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "<application-id>",
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/<directory-id>/oauth2/token")
// Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)
```

- 需要替代的内容：
  - `<application-id>`与Azure Active Directory应用程序的**应用程序（客户端）ID**一起使用。
  - `<scope-name>` 与Databricks秘密作用域名称一起使用。
  - `<service-credential-key-name>` 包含包含客户机密的密钥的名称。
  - `<directory-id>`与Azure Active Directory应用程序的**目录（租户）ID**一起使用。
  - `<container-name>` 使用ADLS Gen2存储帐户中的容器名称。
  - `<storage-account-name>` 使用ADLS Gen2存储帐户名。
  - `<mount-name>` 在DBFS中使用预期的挂载点的名称。
- 实际测试报错的代码

```scala
%scala
configs = {"fs.azure.account.auth.type": "OAuth",
 "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
 "fs.azure.account.oauth2.client.id": "",
 "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<-->",key="<-->"),
 "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<-->/oauth2/token"}

# 连接Azure Data Lake Storage Gen 2
# Storage account = blob + Azure Data Lake Storage Gen 2, Azure Data Lake Storage Gen 1则是一个单独的服务
dbutils.fs.mount(
  source = "abfss://<-->@<-->.dfs.core.chinacloudapi.cn/",
  mount_point = "/mnt/<-->",
  extra_configs = configs)
```

- 报错情况

![image-20210422170952003](https://i.loli.net/2021/04/22/pdu857R6BOx21VX.png)

### 错误的原因：官方文档有误

- 报错情况

![image-20210422171025747](https://i.loli.net/2021/04/22/wmARxYSJzvs7EqT.png)

- 官方文档地址：https://docs.databricks.com/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access.html
- 下面查看源码

```scala
import dbutils

print(dir(dbutils))
print(dbutils.__file__)

['Console', 'DBUtils', 'FileInfo', 'Iterable', 'ListConverter', 'MapConverter', 'MountInfo', 'NotebookExit', 'Py4JJavaError', 'SecretMetadata', 'SecretScope', 'WidgetsHandlerImpl', '__builtins__', '__cached__', '__doc__', '__file__', '__loader__', '__name__', '__package__', '__spec__', '__warningregistry__', 'absolute_import', 'getCurrentCredentials', 'makeTensorboardManager', 'namedtuple', 'print_function', 'range', 'stderr', 'stdout', 'string_types', 'sys', 'zip']
/local_disk0/tmp/1617768854981-0/dbutils.py

# 拿到dbutils.py的源码
%sh cat /local_disk0/tmp/1617768854981-0/dbutils.py
```

![image-20210409111945411](https://i.loli.net/2021/04/09/5gkzFWEJ7MTo1dn.png)

### 正确方法：直接用sakey连接的方式

```SPARQL
# Set up direct account access key
sakey = '<>'
adlsname = '<>'
adlscontainer = '<>'
adlsloc = 'abfss://' + adlscontainer + '@' + adlsname + '.dfs.core.chinacloudapi.cn/'
spark.conf.set('fs.azure.account.key.' + adlsname + '.dfs.core.chinacloudapi.cn',sakey)
abfss://<>@<>.dfs.core.chinacloudapi.cn/
```

![image-20210422171132105](https://i.loli.net/2021/04/22/G5FhoDCZ4W2wgYy.png)

- 注意全部要替换成`chinacloudapi.cn`
- sakey：storage account的访问密钥

![image-20210409104948398](https://i.loli.net/2021/04/09/qgCbn4oSH19Pday.png)

- adlsname：storage account的名字

![image-20210409105028896](https://i.loli.net/2021/04/09/qCEKxjcMA9i3mJV.png)

- adlscontainer：容器的名字

![image-20210409105147966](https://i.loli.net/2021/04/09/HUo5RFjPL7eVqZv.png)

```scala
val configs = Map(
  "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
  "dfs.adls.oauth2.client.id" -> "<>",
  "dfs.adls.oauth2.credential" -> "<>",
  "dfs.adls.oauth2.refresh.url" -> "https://<>.databricks.azure.cn/<>/oauth2/token")

dbutils.fs.mount(
  "abfss://<>@<>.dfs.core.chinacloudapi.cn/",
   "/mnt/", "", null, configs)
```

### 测试结果：连接成功

```python
df = spark.read.parquet(adlsloc + '<>')
#df = spark.read.text("dbfs:/mnt/<mount-name>/....")
```

![image-20210422171248827](https://i.loli.net/2021/04/22/2hwRCDsoEjF61cK.png)

### 断开连接

```python
dbutils.fs.unmount("/mnt/<mount-name>")
```

## 1.2 CDH连接ADLS

### 使用 ACL 对文件系统执行操作

获取有关授予 服务主体访问该帐户的权限的信息。服务主体应具有读取，写入和执行权限。（RBAC可以跳过）

参考：https://docs.microsoft.com/zh-cn/azure/data-lake-store/data-lake-store-security-overview#using-acls-for-operations-on-file-systems

### 在Cloudera Manager中配置ADLS凭证

要使用ADLS连接到CDH集群必须要配置Hadoop Core-site.xml

![image-20210422171319500](https://i.loli.net/2021/04/22/MTSf9E1yC8gJ2hU.png)

### 添加Core-site.xml属性

| 名称                                    | 值                                                           |
| --------------------------------------- | ------------------------------------------------------------ |
| fs.azure.account.auth.type              | OAuth                                                        |
| fs.azure.account.oauth.provider.type    | org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider |
| fs.azure.account.oauth2.client.endpoint | https://login.chinacloudapi.cn/<Tenant_ID>/oauth2/token      |
| fs.azure.account.oauth2.client.secret   | < Client_Secret>                                             |
| fs.azure.account.oauth2.client.id       | < Client_ID >                                                |

保存变更，并重启 HDFS，使得变更生效

注意： ADLS Gen2 的 OAuth 2.0 配置必须使用 Azure Active Directory （v1.0）endpoint。当前不支持 Microsoft 身份平台（v2.0）的 endpoint。有 关这两种终结点类型之间差异的更多信息，请参见：[为何更新为 Microsoft 标识平台 (v2.0) | Microsoft Docs](https://docs.microsoft.com/zh-cn/azure/active-directory/azuread-dev/azure-ad-endpoint-comparison)



### 验证 CDH 使用 ADLS Gen2



## 1.3 HDI hive metasotre 更换mysql

1. Create HDInsight Cluster

2. Login head node of new cluster and install mysql server (MySQL 5.7 verified)

   Document: https://dev.mysql.com/doc/mysql-apt-repo-quick-guide/en/

   ```shell
   sudo apt-get -y install mysql-server mysql-connector-java
   sudo ambari-server setup --jdbc-db=mysql --jdbc-driver=/usr/share/java/mysql-connector-java-8.0.23.jar
   ```

3. Configure MySQL for listening VNET IP address

   ```shell
   sudo vim /etc/mysql/mysql.conf.d/mysqld.cnf
   [mysqld]
   bind-address = <>
   ```

4. Create MySQL user for Hive meta store

   ```sql
   CREATE USER hive@'%' IDENTIFIED BY '<>';
   GRANT ALL PRIVILEGES ON *.* TO hive@'%';
   
   FLUSH PRIVILEGES;
   
   CREATE SCHEMA hive;
   ```

5. Login Ambari and configure Hive meta store
   Note: This version HDI only support JDBC driver ‘com.mysql.jdbc.Driver’ for Hive meta store. If you set ‘com.mysql.cj.jdbc.Driver’ as driver class, the Hive services will failed.  

   ![image-20210422171359914](https://i.loli.net/2021/04/22/rUxsd4ym39ADpjg.png)

6. Initialize Hive meta store database for Hive service (This tool will access the meta store database configured in hive-site.xml, so update the configuration in Ambari before this action)  

   ```shell
   # schematool的路径可能不同
   schematool -dbType mysql -initSchema
   
   # 如果有权限问题的报错，可能会需要用到以下命令
   sudo chmod 777 /usr/hdp/current/hive-server2/conf/hive-site.jceks
   ```

7. Restart Hive services  

8. Try create hive in HDI, and check the result in mysqlvm

   ![image-20210422141212313](https://i.loli.net/2021/04/22/vXplGT9aMCAqb7t.png)

## 1.4 CDH hive metastore 更换mysql

### Login Cloudera Manager and configure Hive meta store

![image-20210422171420306](https://i.loli.net/2021/04/22/25dZDnTqbFJuflA.png)

### Login head node of new cluster and install mysql server

```shell
//HDI ambari
sudo apt-get -y install mysql-server mysql-connector-java
sudo ambari-server setup --jdbc-db=mysql --jdbc-driver=/usr/share/java/mysql-connector-java.jar
```

![image-20210422144150482](https://i.loli.net/2021/04/22/tV1BfwOYegLjFT4.png)

```shell
//mysql-connector-java-5.1.46.jar
+ JDBC_JARS=/usr/share/java/mysql-connector-java.jar:/opt/cloudera/cm/lib/postgresql-42.1.4.jre7.jar:/usr/share/java/oracle-connector-java.jar
+ [[ -z /opt/cloudera/cm ]]
+ JDBC_JARS_CLASSPATH='/opt/cloudera/cm/lib/*:/usr/share/java/mysql-connector-java.jar:/opt/cloudera/cm/lib/postgresql-42.1.4.jre7.jar:/usr/share/java/oracle-connector-java.jar'
++ /usr/java/jdk1.8.0_181-cloudera/bin/java -Djava.net.preferIPv4Stack=true -cp '/opt/cloudera/cm/lib/*:/usr/share/java/mysql-connector-java.jar:/opt/cloudera/cm/lib/postgresql-42.1.4.jre7.jar:/usr/share/java/oracle-connector-java.jar' com.cloudera.cmf.service.hive.HiveMetastoreDbUtil /var/run/cloudera-scm-agent/process/353-hive-metastore-create-tables/metastore_db_py.properties unused --printTableCount
log4j:ERROR Could not find value for key log4j.appender.A
log4j:ERROR Could not instantiate appender named "A".
Thu Apr 22 06:41:19 UTC 2021 WARN: Establishing SSL connection without server's identity verification is not recommended. According to MySQL 5.5.45+, 5.6.26+ and 5.7.6+ requirements SSL connection must be established by default if explicit option isn't set. For compliance with existing applications not using SSL the verifyServerCertificate property is set to 'false'. You need either to explicitly disable SSL by setting useSSL=false, or set useSSL=true and provide truststore for server certificate verification.
Thu Apr 22 06:41:19 UTC 2021 WARN: Establishing SSL connection without server's identity verification is not recommended. According to MySQL 5.5.45+, 5.6.26+ and 5.7.6+ requirements SSL connection must be established by default if explicit option isn't set. For compliance with existing applications not using SSL the verifyServerCertificate property is set to 'false'. You need either to explicitly disable SSL by setting useSSL=false, or set useSSL=true and provide truststore for server certificate verification.
+ NUM_TABLES=59
+ [[ 0 -ne 0 ]]
+ [[ 59 -ne 0 ]]
+ echo 'Database already has tables. Skipping table creation.'
+ exit 0
```



![image-20210422145513761](https://i.loli.net/2021/04/22/7MtuHNhn3qZID6c.png)

## 

# 2 Test Cases功能验证

## 2.1 CDH内部交互验证

```sql
-- 在impala-shell环境执行SQL,创建kudu表:

CREATE TABLE tmp_customer (
tenant_id INT NOT NULL ,
id STRING NOT NULL ,
version BIGINT NULL ,
birthday BIGINT NULL ,
city STRING NULL ,
code STRING NULL ,
company STRING NULL ,
country STRING NULL ,
county STRING NULL ,
date_created BIGINT NULL ,
date_join BIGINT NULL ,
annual_revenue DOUBLE NULL ,
employee_number INT NULL ,
PRIMARY KEY (tenant_id, id)
)
PARTITION BY HASH (tenant_id, id) PARTITIONS 16
STORED AS KUDU;
```

```sql
-- 在impala-shell环境执行SQL,插入数据到kudu表:

insert into tmp_customer values
(1,'10001',0,1617022680606,'北京','C001',null,'中国','海淀区',1617022680606,1617022680606,123.456,789),
(1,'10002',0,1617022680606,'上海','C002',null,'中国','徐汇区',1617022680606,1617022680606,111.222,333);
```

## 2.2 Databricks与CDH交互验证

```sql
-- 在hive命令环境执行SQL,创建hive表并保存数据到adls目录:

CREATE EXTERNAL TABLE tmp_list_member (customer_id STRING)
PARTITIONED BY (tenant_id INT,list_id BIGINT)
STORED AS PARQUET
location 'adls://xxx@yyy.zzz.cn/tmp_list_member'; --指定location到ADLS目录
```

```sql
-- 在impala-shell环境执行SQL,查询kudu表数据并插入adls存储的hive表:

insert overwrite table tmp_list_member partition(tenant_id=1,list_id=123)
select id from tmp_customer t
cross join (select 1 union all select 1) t1
```

```sql
-- 通过jdbc连接impala, 执行SQL,同时查询kudu表和hive表:

select count(*),count(distinct customer_id) from tmp_list_member l
left outer join tmp_customer c on l.customer_id=c.id where c.id is not null;
```

```sql
-- 在databricks环境注册kudu表为spark临时表:

spark.read.format("org.apache.kudu.spark.kudu")
.options(Map("kudu.master" -> "10.0.0.1:7051", "kudu.table" -> "impala::default.tmp_customer"))
.load
.createOrReplaceTempView("tmp_customer")


-- 查询kudu表数据并插入hive表:

insert overwrite table tmp_list_member partition(tenant_id=1,list_id=456)
select id from tmp_customer t
cross join (select 1 union all select 1 union all select 1) t1


-- 同时查询kudu表和hive表:

select count(*),count(distinct customer_id) from tmp_list_member l
left semi join tmp_customer c on l.customer_id=c.id;
```

```sql
-- 在databricks环境使用java api连接hbase:

Configuration config = HBaseConfiguration.create();
Connection connection = ConnectionFactory.createConnection(config);
TableName tableName = TableName.valueOf("user");
Table table = connection.getTable(tableName);
String rowkey = "rowkey_10";
Put put= new Put(Bytes.toBytes(rowKey));
put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("张三"));
put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("address"), Bytes.toBytes("北京市"))
table.put(put);
table.close();



-- 在databricks环境使用hadoop的api,把RDD[(ImmutableBytesWritable,KeyValue)]数据生成hfile,bulkload到hbase:

def saveToHbase(hRows:RDD[(ImmutableBytesWritable,KeyValue)],hbase_table:String,config:Config,tmpPath:String) = {
val conf = HBaseConfiguration.create()
conf.set("hbase.mapreduce.hfileoutputformat.table.name", hbase_table)
conf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", "1024")
conf.set("hbase.zookeeper.quorum", config.getString("confHbase.default.zookeeper.quorum"))
conf.set("hbase.zookeeper.property.clientPort", config.getString("confHbase.default.zookeeper.clientPort"))
conf.set("hbase.zookeeper.master", config.getString("confHbase.default.zookeeper.master"))
conf.set("zookeeper.znode.parent", config.getString("confHbase.default.zookeeper.znode.parent"))
val con: Connection = ConnectionFactory.createConnection(conf)
val tableName=TableName.valueOf(hbase_table)
val table: Table = con.getTable(tableName)
val admin=con.getAdmin()
val regionLocator: RegionLocator = con.getRegionLocator(tableName)
val hdfsConf = new Configuration()
hdfsConf.set("fs.defaultFS", config.getString("confHbase.default.defaultFS"))
val hdfs = FileSystem.get(hdfsConf)
val path = new Path(tmpPath)
hdfs.delete(path,true)
hRows.saveAsNewAPIHadoopFile(
tmpPath,
classOf[ImmutableBytesWritable],
classOf[KeyValue],
classOf[HFileOutputFormat2],
conf)
val bulkLoader = new LoadIncrementalHFiles(conf)
bulkLoader.doBulkLoad(path, admin, table, regionLocator)
}
```

- 通过jdbc连接使用mysql

![image-20210422171459030](https://i.loli.net/2021/04/22/fKM4vxHuqTn6Yrk.png)

## 2.3 HDI与CDH交互验证

```sql
-- 在hive命令环境执行SQL,创建hive表并保存数据到adls目录:

CREATE EXTERNAL TABLE tmp_list_member (customer_id STRING)
PARTITIONED BY (tenant_id INT,list_id BIGINT)
STORED AS PARQUET
location 'adls://xxx@yyy.zzz.cn/tmp_list_member'; --指定location到ADLS目录
```