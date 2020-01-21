# hadoopHelp
hadoopHelp使用介绍
项目地址：https://git.newegg.org/ay05/hadoophelp.git
可用功能：提供hdfs的文件上传、删除、下载以及文件夹的创建和验证是否已存在
使用方式：
将项目打包生产jar
通过java -jar 启动jar包
输入help可以看到，工具具有以下命令
  delete: delete
  download: download
  exist: existPath
  mkdir: mkdir
  upload-file: uploadFile
4.使用demo

exist --user bigdata --fsDefaultName hdfs://10.16.236.126:9000 --path hdfs://10.16.236.126:9000/etljob/customer_review/schema/avro


mkdir --user bigdata --fsDefaultName hdfs://10.16.236.126:9000 --path hdfs://10.16.236.126:9000/etljob/customer_review/schema/avro2


upload-file --user bigdata --fsDefaultName hdfs://10.16.236.126:9000 --srcPath /home/buyabs.corp/ay05/aiden/temp.txt --dstPath /etljob/customer_review/schema/avro/temp.txt


delete --user bigdata --fsDefaultName hdfs://10.16.236.126:9000 --path hdfs://10.16.236.126:9000/etljob/customer_review/schema/avro/temp.txt


download --user bigdata --fsDefaultName hdfs://10.16.236.126:9000 --srcPath hdfs://10.16.236.126:9000/etljob/customer_review/schema/avro/HBase_EC_ItemR
