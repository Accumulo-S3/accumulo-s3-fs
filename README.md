# AccumuloS3FS
Accumulo classpath extension to support using S3 as the underlying file system.

This project is intended to add S3 support to older versions (1.9.3, 2.0.1) of Accumulo that aren't compatible with the S3AFileSystem defined in hadoop-aws. This implementation was heavily influeced by Chris Milbert's implementation of Accumulo with S3 support (https://github.com/cmilbert/accumulo). Unlike his version though this is a slimmed down implementation that doesn't change any of the baseline Accumulo code. Instead this version is deployed as a classpath extension to release versions of Accumulo. 

Deployment Steps:
1. Build the accumulo-s3-fs artifact and add the jar to the `lib` directory of an existing Accumulo deployment. Also add the aws-java-sdk-bundle (version 1.12.83) jar to the `lib` directory of Accumulo. Downloaded here: https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.83/aws-java-sdk-bundle-1.12.83.jar
2. Update the following values to your `accumulo.properties` file:
```
  instance.volumes=accS3nf://YOUR_S3_BUCKET/accumulo,accS3mo://YOUR_S3_BUCKET/accumulo-wal
  general.volume.chooser=org.apache.accumulo.server.fs.PreferredVolumeChooser
  general.custom.volume.preferred.default=accS3nf://{{ YOUR_S3_BUCKET }}/accumulo
  general.custom.volume.preferred.logger=accS3mo://{{ YOUR_S3_BUCKET }}/accumulo-wal
  master.walog.closer.implementation=org.apache.accumulo.s3.manager.S3LogCloser
  ```
  3. Add a `core-site.xml` file to the conf directory of your accumulo deployment with the following properties:
  ```
  <configuration>
        <property>
            <name>fs.defaultFS</name>
            <value>accS3nf://{{ YOUR_S3_BUCKET }}</value>
        </property>
        <property>
            <name>fs.accS3nf.impl</name>
            <value>org.apache.accumulo.s3.file.AccumuloNoFlushS3FileSystem</value>
        </property>
        <property>
            <name>fs.accS3mo.impl</name>
            <value>org.apache.accumulo.s3.file.AccumuloMultiObjectS3FileSystem</value>
        </property>
        <property>
            <name>fs.s3a.access.key</name>
            <value>{{ YOUR_S3_KEY }}</value>
        </property>
        <property>
            <name>fs.s3a.secret.key</name>
            <value>{{ YOUR_S3_SECRET }}</value>
        </property>
        
        // IF DEPLOYING TO A CUSTOM ENDPOINT/S3 DEPLOYMENT
        <property>
            <name>fs.s3.endpoint</name>
            <value>{{ YOUR_S3_ENDPOINT_URL }}</value>
        </property>
        
        <!--
        // OPTIONAL REGION PROPERTY
        <property>
            <name>fs.s3.region</name>
            <value>{{ YOUR_S3_REGION }}</value>
        </property>
        // IF YOUR S3 DEPLOYMENT'S ENDPOINT IS'T CONFIGURED TO USE SSL YOU CAN DISABLE IT IN THE CLIENT
        <property>
          <name>fs.s3a.connection.ssl.enabled</name>
          <value>false</value>
        </property>-->
    </configuration>
  ```
