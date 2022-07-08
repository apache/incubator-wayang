/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.wayang.core.util.fs;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.wayang.core.api.exception.WayangException;

public class S3FileSystem implements FileSystem {

  private AmazonS3 s3;

  final Map<String, S3Pair> pairs = new HashMap<>();

  public S3FileSystem(){}

  public static void main(String... args) throws IOException {
    S3FileSystem s3 = new S3FileSystem();
    //String url = "s3://blossom-benchmark/HIGGS.csv";
   // String url = "s3://blossom-benchmark/README.md";
    String url = "s3://blossom-benchmark/lulu/lolo/lala";
    System.out.println(url);
    System.out.println(s3.getS3Pair(url).getBucket());
    System.out.println(s3.getS3Pair(url).getKey());
    System.out.println(s3.preFoldersExits(s3.getS3Pair(url)));

   // System.out.println(s3.getFileSize(url));
//    InputStream content = s3.open(url);
//    new BufferedReader(new InputStreamReader(content)).lines().forEach(System.out::println);
//    System.out.println(s3.listChildren(url));
//    System.out.println(s3.isDirectory(url));
    OutputStream output = s3.create(url, true);
    byte[] bytes = "lala".getBytes();
    output.write(bytes);
    output.flush();
    output.close();
  }

  private AmazonS3 getS3Client(){
    if(this.s3 == null){
      if(
          System.getProperties().contains("fs.s3.awsAccessKeyId") &&
          System.getProperties().contains("fs.s3.awsSecretAccessKey")
      ){
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            System.getProperty("fs.s3.awsAccessKeyId"),
            System.getProperty("fs.s3.awsSecretAccessKey")
        );
        this.s3 = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .build();
      }else{
        this.s3 = AmazonS3ClientBuilder.defaultClient();
      }
    }
    return this.s3;
  }

  class S3Pair{

    private final String bucket;
    private final String key;

    public S3Pair(S3FileSystem s3Client, String url){
      if( ! s3Client.canHandle(url)){
        throw new WayangException("The files can not be handle by "+this.getClass().getSimpleName());
      }
      String[] parts = url.split("/", 4);
      String key_tmp = "";
      if(parts.length == 4) {
        key_tmp = parts[3];
      }
      this.bucket = parts[2];
      this.key = key_tmp;
    }

    public S3Pair(String bucket, String key){
      this.bucket = bucket;
      this.key = key;
    }

    public String getBucket() {
      return bucket;
    }

    public String getKey() {
      return key;
    }
  }

  private S3Pair getS3Pair(String url){
    S3Pair pair = this.pairs.get(url);
    if(pair == null){
      pair = new S3Pair(this, url);
      this.pairs.put(url, pair);
    }
    return pair;
  }

  @Override
  public long getFileSize(String fileUrl) throws FileNotFoundException {
    return this.getFileSize(this.getS3Pair(fileUrl));
  }

  private long getFileSize(S3Pair pair) throws FileNotFoundException {
    return this.getS3Client().getObjectMetadata(pair.getBucket(), pair.getKey()).getContentLength();
  }

  @Override
  public boolean canHandle(String url) {
    String url_lower = url.substring(0, 5).toLowerCase();
    return url_lower.startsWith("s3a:/");
  }

  @Override
  public InputStream open(String url) throws IOException {
    return this.open(this.getS3Pair(url));
  }

  private InputStream open(S3Pair pair) throws IOException {
    return this.getS3Client().getObject(pair.getBucket(), pair.getKey()).getObjectContent();
  }

  @Override
  public OutputStream create(String url) throws IOException {
    return this.create(this.getS3Pair(url));
  }

  private OutputStream create(S3Pair pair) throws IOException {
    return this.create(pair, false);
  }

  @Override
  public OutputStream create(String url, Boolean forceCreateParentDirs) throws IOException {
    return this.create(this.getS3Pair(url), forceCreateParentDirs);
  }

  private OutputStream create(S3Pair pair, Boolean forceCreateParentDirs) throws IOException {
    if( ! forceCreateParentDirs ){
      if ( ! this.preFoldersExits(pair) )
        throw new IOException(
            String.format(
              "The folder '%s' does not exist in the bucket '%s'",
              pair.getKey(),
              pair.getBucket()
            )
        );
    }

    PipedInputStream in = new PipedInputStream();
    final PipedOutputStream out = new PipedOutputStream(in);

    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType("text/plain");
    AmazonS3 s3Client = this.getS3Client();
    new Thread(new Runnable() {
      public void run() {
        PutObjectResult result = s3Client.putObject(pair.getBucket(), pair.getKey(), in, metadata);
      }
    }).start();
    return out;
  }

  public boolean bucketExits(S3Pair pair){
    return this.getS3Client().doesBucketExistV2(pair.getBucket());
  }

  public boolean preFoldersExits(S3Pair pair){
    if( ! this.getS3Client().doesBucketExistV2(pair.getBucket()) ) return false;
    String[] keys = pair.getKey().split("/");
    String aggregated = "";
    for(int i = 0; i < keys.length; i++){
      aggregated = aggregated + "/" +keys[i];
      if( ! isDirectory(new S3Pair(pair.getBucket(), aggregated)) ){
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isDirectory(String url) {
    return this.isDirectory(this.getS3Pair(url));
  }

  private boolean isDirectory(S3Pair pair) {
    if( ! this.bucketExits(pair) ) return false;

    String key = pair.getKey();
    long size = listChildren(pair).stream().filter(name -> ! name.equals(key)).count();
    if(size > 0){
      return true;
    }
    return false;
  }

  @Override
  public Collection<String> listChildren(String url) {
    return this.listChildren(this.getS3Pair(url));
  }

  private Collection<String> listChildren(S3Pair pair) {
    ObjectListing listing = this.getS3Client().listObjects(pair.getBucket(), pair.getKey());
    return listing.getObjectSummaries().stream()
        .map(obj -> obj.getKey())
        .collect(Collectors.toList());
  }

  @Override
  public boolean delete(String url, boolean isRecursiveDelete) throws IOException {
    return this.delete(this.getS3Pair(url), isRecursiveDelete);
  }

  private boolean delete(S3Pair pair, boolean isRecursiveDelete) throws IOException {
    if(!isRecursiveDelete){
      if(isDirectory(pair)){
        throw new IOException("the path correspond to a directory");
      }
    }
    this.getS3Client().deleteObject(pair.getBucket(), pair.getKey());
    return true;
  }
}
