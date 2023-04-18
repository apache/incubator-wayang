This tutorial will show users how to run WordCount example locally with Wayang.

# Get the binary
Running following command to build Wayang and generate the tar.gz  
```shell
cd incubator-wayang
./mvnw clean package -pl :wayang-assembly -Pdistribution 
```
Then you can find the `wayang-assembly-0.6.1-SNAPSHOT-dist.tar.gz` under `wayang-assembly/target` directory.


# Prepare the environment
## Wayang
```shell
tar -xvf wayang-assembly-0.6.1-SNAPSHOT-dist.tar.gz
cd wayang-0.6.1-SNAPSHOT
```

In linux
```shell 
echo "export WAYANG_HOME=$(pwd)" >> ~/.bashrc
echo "export PATH=${PATH}:${WAYANG_HOME}/bin" >> ~/.bashrc
source ~/.bashrc
```
In MacOS
```shell 
echo "export WAYANG_HOME=$(pwd)" >> ~/.zshrc
echo "export PATH=${PATH}:${WAYANG_HOME}/bin" >> ~/.zshrc
source ~/.zshrc
```
## Others
- You need to install Apache Spark version 3 or higher. Don’t forget to set the `SPARK_HOME` environment variable.
- You need to install Apache Hadoop version 3 or higher. Don’t forget to set the `HADOOP_HOME` environment variable.

# Run the program

To execute the WordCount example with Apache Wayang, you need to execute your program with the 'wayang-submit' command:

```shell
cd wayang-0.6.1-SNAPSHOT
wayang-submit org.apache.wayang.apps.wordcount.Main java file://$(pwd)/README.md
```
Then you should be able to see outputs like this:
![img.png](images/wordcount.png)