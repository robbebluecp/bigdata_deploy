import os
import tarfile
import wget
import re
import shutil

# 根路径
root = '/opt2'

config = {
    # master
    'master': 'sp',
    # slaves, 以列表形式存储
    'slaves': ['sp1'],
    # 块大小
    'dfs.blocksize': 60000000,
    # namenode direction
    'dfs.namenode.name.dir': root + '/bigdata/hadoop/hadoop_namenode',
    # datanode direction
    'dfs.datanode.data.dir': root + '/bigdata/hadoop/hadoop_datanode',
    # 副本集个数
    'dfs.replication': 1,
    # yarn集群端口
    'yarn.resourcemanager.webapp.address.port': 19405,
    # spark集群端口
    'SPARK_MASTER_WEBUI_PORT': 19406
}

# ============以上配置必须得配！必须得配！必须得配！我没有设置默认值==============#
# ============以上配置必须得配！必须得配！必须得配！我没有设置默认值==============#
# ============以上配置必须得配！必须得配！必须得配！我没有设置默认值==============#

# 默认在根目录下在建一个bigdata文件夹
try:
    os.makedirs(root + '/bigdata/')
except:
    pass

root = root + '/bigdata/'
config.update({'root': root})

# java_home也得自己先配好，这个目前还不知道怎么用脚本配，因为oracle安装jdk需要token
process = os.popen('echo $JAVA_HOME')  # return file
JAVA_HOME = process.read()
process.close()
# assert JAVA_HOME.find('jdk') >= 0, 'JAVA_HOME配置有问题，请重新配置'
print("$JAVA_HOME=" + JAVA_HOME + '\n')

config.update({'JAVA_HOME': JAVA_HOME})


# 下载hadoop、spark、scala、mysql-connector
def soft_download():
    print('安装hadoop。。。\n')
    if not os.path.exists(root + 'hadoop'):
        os.mkdir(root + 'hadoop')
        wget.download('http://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-3.1.2/hadoop-3.1.2.tar.gz', root + 'hadoop/hadoop.tar.gz')
        tar = tarfile.open(root + 'hadoop/hadoop.tar.gz', 'r')
        tar.extractall(root + 'hadoop')
        for i in os.listdir(root + 'hadoop'):
            if i.find('gz') < 0:
                os.rename(root + 'hadoop/' + i, root + 'hadoop/hadoop_main')
                break

    print('安装spark。。。\n')
    if not os.path.exists(root + 'spark'):
        os.mkdir(root + 'spark')
        wget.download('http://mirror.dsrg.utoronto.ca/apache/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz', root + 'spark/spark.tgz')
        tar = tarfile.open(root + 'spark/spark.tgz', 'r')
        tar.extractall(root + 'spark')
        for i in os.listdir(root + 'spark'):
            if i.find('gz') < 0:
                os.rename(root + 'spark/' + i, root + 'spark/spark_main')
                break

    print('安装scala。。。\n')
    if not os.path.exists(root + 'scala'):
        os.mkdir(root + 'scala')
        wget.download('https://downloads.lightbend.com/scala/2.13.0/scala-2.13.0.tgz', root + 'scala/scala.tgz')
        tar = tarfile.open(root + 'scala/scala.tgz', 'r')
        tar.extractall(root + 'scala')
        for i in os.listdir(root + 'scala'):
            if i.find('gz') < 0:
                os.rename(root + 'scala/' + i, root + 'scala/scala_main')
                break

    print('安装mysql-connector。。。\n')
    if not os.path.exists(root + 'mysql_connector'):
        os.mkdir(root + 'mysql_connector')
        wget.download('https://cdn.mysql.com//Downloads/Connector-J/mysql-connector-java_8.0.16-1ubuntu16.04_all.deb', root + 'mysql_connector/mysql_connector.deb')
        os.system("""dpkg -X %s %s""" % (root + 'mysql_connector/mysql_connector.deb', root + 'mysql_connector'))


# 配置hadoop
def config_hadoop():

    # etc/hadoop/core-site.xml
    with open(root + 'hadoop/hadoop_main/etc/hadoop/core-site.xml', 'r') as f:
        text = f.read()
        f.close()
    text = re.sub('<configuration>[\s\S]+?</configuration>', '''<configuration>
    
                                                                <property>
                                                                      <name>fs.defaultFS</name>
                                                                      <value>hdfs://%(master)s:9000</value>
                                                                  </property>
                                                                
                                                                  <property>
                                                                      <name>io.file.buffer.size</name>
                                                                      <value>131072</value>
                                                                  </property>
                                                                  
                                                                </configuration>''' % config, text)
    with open(root + 'hadoop/hadoop_main/etc/hadoop/core-site.xml', 'w') as f:
        f.write(text)
        f.close()

    # etc/hadoop/hdfs-site.xml
    with open(root + 'hadoop/hadoop_main/etc/hadoop/hdfs-site.xml', 'r') as f:
        text = f.read()
        f.close()

    text = re.sub('<configuration>[\s\S]+?</configuration>', '''<configuration>
                                                                    <property>
                                                                      <name>dfs.namenode.name.dir</name>
                                                                      <value>%(dfs.namenode.name.dir)s</value>
                                                                    </property>
                                                                    
                                                                    <property>
                                                                      <name>dfs.blocksize</name>
                                                                      <value>%(dfs.blocksize)s</value>
                                                                    </property>
                                                                    
                                                                    <property>
                                                                      <name>dfs.namenode.handler.count</name>
                                                                      <value>100</value>
                                                                    </property>
                            
                                                                    
                                                                    <property>
                                                                      <name>dfs.datanode.data.dir</name>
                                                                      <value>%(dfs.datanode.data.dir)s</value>
                                                                    </property>
                                                                    
                                                                    <property>
                                                                        <name>dfs.replication</name>
                                                                        <value>%(dfs.replication)s</value>
                                                                    </property>
                                                                
                                                                </configuration>''' % config, text)
    with open(root + 'hadoop/hadoop_main/etc/hadoop/hdfs-site.xml', 'w') as f:
        f.write(text)
        f.close()

    # etc/hadoop/yarn-site.xml
    with open(root + 'hadoop/hadoop_main/etc/hadoop/yarn-site.xml', 'r') as f:
        text = f.read()
        f.close()

    text = re.sub('<configuration>[\s\S]+?</configuration>', '''<configuration>
    
                                                                    <property>
                                                                      <name>yarn.resourcemanager.hostname</name>
                                                                      <value>%(master)s</value>
                                                                    </property>
                                                                    
                                                                    <property>
                                                                      <name>yarn.resourcemanager.webapp.address</name>
                                                                      <value>0.0.0.0:%(yarn.resourcemanager.webapp.address.port)s</value>
                                                                    </property>
                                                                    
                                                                    <property>
                                                                      <name>yarn.nodemanager.aux-services</name>
                                                                      <value>mapreduce_shuffle</value>
                                                                    </property>

                                                                </configuration>''' % config, text)
    with open(root + 'hadoop/hadoop_main/etc/hadoop/yarn-site.xml', 'w') as f:
        f.write(text)
        f.close()

    # etc/hadoop/mapred-site.xml
    with open(root + 'hadoop/hadoop_main/etc/hadoop/mapred-site.xml', 'r') as f:
        text = f.read()
        f.close()

    text = re.sub('<configuration>[\s\S]+?</configuration>', '''<configuration>

                                                                    <property>
                                                                       <name>mapreduce.framework.name</name>
                                                                       <value>yarn</value>
                                                                    </property>
                                                                </configuration>''', text)

    with open(root + 'hadoop/hadoop_main/etc/hadoop/mapred-site.xml', 'w') as f:
        f.write(text)
        f.close()

    # etc/hadoop/hadoop-evn.sh
    with open(root + 'hadoop/hadoop_main/etc/hadoop/hadoop-env.sh.xml', 'a') as f:
        f.write('''\n
export JAVA_HOME=%(JAVA_HOME)s
export HDFS_NAMENODE_USER="root"
export HDFS_DATANODE_USER="root"
export HDFS_SECONDARYNAMENODE_USER="root"
export HADOOP_SECURE_DN_USER=hdfs
export YARN_RESOURCEMANAGER_USER="root"
export YARN_NODEMANAGER_USER="root"
export HADOOP_SECURE_DN_USER=yarn
        ''' % config)
        f.close()

    # etc/hadoop/workers
    with open(root + 'hadoop/hadoop_main/etc/hadoop/workers', 'w') as f:
        f.write('\n'.join(config['slaves']) + '\n' + config['master'])
        f.close()


def config_spark():

    # conf/spark-env.sh
    shutil.copy(root + 'spark/spark_main/conf/spark-env.sh.template', root + 'spark/spark_main/conf/spark-env.sh')
    with open(root + 'spark/spark_main/conf/spark-env.sh', 'a') as f:
        f.write('''
export JAVA_HOME=%(JAVA_HOME)s
export SPARK_MASTER_IP=%(master)s
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=%(SPARK_MASTER_WEBUI_PORT)s
        ''' % config)
        f.close()


    # conf/slaves
    shutil.copy(root + 'spark/spark_main/conf/slaves.template', root + 'spark/spark_main/conf/slaves')
    with open(root + 'spark/spark_main/conf/slaves', 'w') as f:
        f.write('\n'.join(config['slaves']) + '\n' + config['master'])
        f.close()

    # copy mysql-connector into spark jar lib
    shutil.copy(root + 'mysql_connector/usr/share/java/mysql-connector-java-8.0.16.jar', root + 'spark/spark_main/jars/mysql-connector-java-8.0.16.jar')

def config_bashrc():
    with open('/root/.bashrc', 'a') as f:
        f.write('''
export SPARK_HOME=%(root)s/spark/spark_main
export PATH=$PATH:${SPARK_HOME}/bin
export PYSPARK_PYTHON=python3
        
export HADOOP_HOME=%(root)s/hadoop/hadoop_main
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
        
export SCALA_HOME=%(root)s/scala/scala_main
export PATH=$PATH:$SCALA_HOME/bin
        ''' % config)
        f.close()


soft_download()
config_hadoop()
config_spark()
config_bashrc()

"""
最后！记得source ~/.bashrc, 因为这一句目前也没法加进脚本里
"""
