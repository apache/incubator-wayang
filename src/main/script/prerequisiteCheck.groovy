import java.util.regex.Matcher

/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 */

allConditionsMet = true

baseDirectory = project.model.pomFile.parent

/*
 Checks if a given version number is at least as high as a given reference version.
*/

def checkVersionAtLeast(String current, String minimum) {
    def currentSegments = current.tokenize('.')
    def minimumSegments = minimum.tokenize('.')
    def numSegments = Math.min(currentSegments.size(), minimumSegments.size())
    for (int i = 0; i < numSegments; ++i) {
        def currentSegment = currentSegments[i].toInteger()
        def minimumSegment = minimumSegments[i].toInteger()
        if (currentSegment < minimumSegment) {
            println current.padRight(14) + " FAILED (required min " + minimum + " but got " + current + ")"
            return false
        } else if (currentSegment > minimumSegment) {
            println current.padRight(14) + " OK"
            return true
        }
    }
    def curNotShorter = currentSegments.size() >= minimumSegments.size()
    if (curNotShorter) {
        println current.padRight(14) + " OK"
    } else {
        println current.padRight(14) + " (required min " + minimum + " but got " + current + ")"
    }
    curNotShorter
}

def checkVersionAtMost(String current, String maximum) {
    def currentSegments = current.tokenize('.')
    def maximumSegments = maximum.tokenize('.')
    def numSegments = Math.min(currentSegments.size(), maximumSegments.size())
    for (int i = 0; i < numSegments; ++i) {
        def currentSegment = currentSegments[i].toInteger()
        def maximumSegment = maximumSegments[i].toInteger()
        if (currentSegment > maximumSegment) {
            println current.padRight(14) + " FAILED (required max " + maximum + " but got " + current + ")"
            return false
        } else if (currentSegment < maximumSegment) {
            println current.padRight(14) + " OK"
            return true
        }
    }
    def curNotShorter = currentSegments.size() >= maximumSegments.size()
    if (curNotShorter) {
        println current.padRight(14) + " OK"
    } else {
        println current.padRight(14) + " (required max " + maximum + " but got " + current + ")"
    }
    curNotShorter
}

def checkJavaVersion(String minVersion, String maxVersion) {
    print "Detecting Java version:    "
    def curVersion = System.properties['java.version']
    def result
    if (minVersion != null) {
        result = checkVersionAtLeast(curVersion, minVersion)
        if (!result) {
            allConditionsMet = false
            return
        }
    }
    if (maxVersion != null) {
        result = checkVersionAtMost(curVersion, maxVersion)
        if (!result) {
            allConditionsMet = false
            return
        }
    }
}

def checkMavenVersion(String minVersion, String maxVersion) {
    print "Detecting Maven version:   "
    def curVersion = project.projectBuilderConfiguration.systemProperties['maven.version']
    def result
    if (minVersion != null) {
        result = checkVersionAtLeast(curVersion, minVersion)
        if (!result) {
            allConditionsMet = false
            return
        }
    }
    if (maxVersion != null) {
        result = checkVersionAtMost(curVersion, maxVersion)
        if (!result) {
            allConditionsMet = false
            return
        }
    }
}

def checkHadoop(String os) {
    print "Checking Hadoop:                          "
    def hadoopHome = System.getenv("HADOOP_HOME")
    if ((hadoopHome == null) || hadoopHome.isEmpty()) {
        println "FAILED (HADOOP_HOME no set)"
        allConditionsMet = false
    } else {
        if(new File(hadoopHome).exists()) {
            // On Windows we additionally need to install the winutils binaries.
            if (os == "windows") {
                if(new File("bin/winutils.exe", new File(hadoopHome)).exists()) {
                    println "OK"
                } else {
                    println "FAILED (HADOOP_HOME/bin is missing winutils.exe. Please get pre-compiled binaries from here: https://github.com/cdarlint/winutils)"
                    allConditionsMet = false
                }
            } else {
                println "OK"
            }
        } else {
            println "FAILED (HADOOP_HOME set to non-existing directory)"
            allConditionsMet = false
        }
    }

}


/**
 * Version extraction function/macro. It looks for occurrence of x.y or x.y.z
 * in passed input text (likely output from `program --version` command if found).
 *
 * @param input
 * @return
 */
private Matcher extractVersion(input) {
    def matcher = input =~ /(\d+\.\d+(\.\d+)?).*/
    matcher
}

/////////////////////////////////////////////////////
// Find out which OS and arch are bring used.
/////////////////////////////////////////////////////

def osString = project.properties['os.classifier']
def osMatcher = osString =~ /(.*)-(.*)/
if (osMatcher.size() == 0) {
    throw new RuntimeException("Currently unsupported OS")
}
def os = osMatcher[0][1]
def arch = osMatcher[0][2]
println "Detected OS:   " + os
println "Detected Arch: " + arch

/////////////////////////////////////////////////////
// Find out which profiles are enabled.
/////////////////////////////////////////////////////

// - Windows:
//     - Check the length of the path of the base dir as we're having issues with the length of paths being too long.
if (os == "windows") {
    File pomFile = project.model.pomFile
    if (pomFile.absolutePath.length() > 100) {
        println "On Windows we encounter problems with maximum path lengths. " +
            "Please move the project to a place it has a shorter base path " +
            "and run the build again."
        allConditionsMet = false;
    }
}

/////////////////////////////////////////////////////
// Do the actual checks depending on the enabled
// profiles.
/////////////////////////////////////////////////////

checkJavaVersion("1.8", "11")

// Check if hadoop is available
// It seems that this is only required on Windows systems.
if (os == "windows") {
    checkHadoop(os)
}

if (!allConditionsMet) {
    throw new RuntimeException("Not all conditions met, see log for details.")
}
println ""
println "All known conditions met successfully."
println ""

// Things we could possibly check:
// - DNS Providers that return a default ip on unknown host-names
// - Availability and version of LibPCAP/WinPCAP