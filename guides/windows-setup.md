<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

nano windows-setup.mdnano windows-setup.md Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Running Apache Wayang on Windows

This guide helps Windows users successfully build and run Apache Wayang.

Many users encounter issues related to Hadoop, winutils, and environment variables.  
Follow these steps carefully.

---

## 1. Install Java 17

Download and install:

https://adoptium.net/

After installation verify:
java -version

---

## 2. Install Maven

Download:

https://maven.apache.org/download.cgi

Extract and add Maven `/bin` to your **System PATH**.

Verify:
mvn -version

---

## 3. Install Hadoop winutils

Wayang requires Hadoop utilities on Windows.

### Download winutils

Download from:

https://github.com/steveloughran/winutils

Choose a version matching Hadoop 3.x.

### Setup

Create directory: C:\hadoop\bin

Place: winutils.exe

inside `bin`.

---

## 4. Set Environment Variables

Open:

**System Properties → Environment Variables**

### Add:

#### HADOOP_HOME
C:\hadoop

#### Add to PATH
C:\hadoop\bin

Restart terminal after saving.

---

## 5. Verify Hadoop setup

Run:
winutils.exe ls


If no error appears → setup is correct.

---

## 6. Build Wayang

From project root:
./mvnw clean install -DskipTests

---

## 7. Common Issues

### ❌ winutils.exe not found
Ensure:

• file exists in `C:\hadoop\bin`  
• PATH includes the bin folder  

### ❌ HADOOP_HOME not set
Verify environment variable.

### ❌ Access denied errors
Run terminal as Administrator.

---

## 8. Notes

• Windows support requires winutils.  
• WSL (Windows Subsystem for Linux) can be used as an alternative.

---

You are now ready to run Apache Wayang on Windows.

### Hadoop prerequisite issue on Windows

While building Wayang on Windows, Maven may fail due to a missing Hadoop dependency.

Error example:
Execution prerequisite-check failed: could not access constructor...

This happens because Wayang expects the file:

C:\hadoop\bin\winutils.exe

Fix:

1. Create folder C:\hadoop
2. Inside create folder bin
3. Inside bin create an empty file called winutils.exe

After this run:

mvn clean install -DskipTests

The build should work correctly.
