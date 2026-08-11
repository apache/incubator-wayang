#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

param(
    [string]$Config = "platforms-setup-guides/duckdb-setup/profiling/ga-relaxed.properties",
    [string]$Executions = "wayang-platforms/wayang-duckdb/target/cost-profiling/duckdb/executions.json",
    [string]$Log = "wayang-platforms/wayang-duckdb/target/cost-profiling/duckdb/ga-relaxed-run.log"
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = (Resolve-Path (Join-Path $scriptDir "../../..")).Path
$mvnw = Join-Path $root "mvnw.cmd"

function Resolve-WayangFileUrl([string]$Path) {
    $resolved = (Resolve-Path (Join-Path $root $Path)).Path
    return ([System.Uri]$resolved).AbsoluteUri
}

function Resolve-RepoPath([string]$Path) {
    return (Resolve-Path (Join-Path $root $Path)).Path
}

function Add-IfExists([System.Collections.Generic.List[string]]$Items, [string]$Path) {
    if (Test-Path $Path) {
        $Items.Add((Resolve-Path $Path).Path)
    }
}

Push-Location $root
try {
    $compileArgs = @(
        "-Pskip-prerequisite-check",
        "-pl", "wayang-profiler,wayang-platforms/wayang-duckdb",
        "-am",
        "-DskipTests",
        "-Drat.skip=true",
        "-Dlicense.skip=true",
        "compile"
    )
    & $mvnw @compileArgs

    if ($LASTEXITCODE -ne 0) {
        throw "Maven compile failed with exit code $LASTEXITCODE."
    }

    $classpathArgs = @(
        "-Pskip-prerequisite-check",
        "-pl", "wayang-profiler",
        "-DincludeScope=runtime",
        "-Dmdep.outputFile=target/duckdb-ga-profiler-classpath.txt",
        "-Drat.skip=true",
        "-Dlicense.skip=true",
        "dependency:build-classpath"
    )
    & $mvnw @classpathArgs

    if ($LASTEXITCODE -ne 0) {
        throw "Maven classpath generation failed with exit code $LASTEXITCODE."
    }

    $dependencyClasspath = Get-Content "wayang-profiler/target/duckdb-ga-profiler-classpath.txt"
    $classpathItems = [System.Collections.Generic.List[string]]::new()

    Add-IfExists $classpathItems "$env:USERPROFILE/.m2/repository/org/antlr/antlr4-runtime/4.13.1/antlr4-runtime-4.13.1.jar"
    Add-IfExists $classpathItems "$env:USERPROFILE/.m2/repository/com/fasterxml/jackson/core/jackson-core/2.18.6/jackson-core-2.18.6.jar"

    foreach ($classesDir in @(
        "wayang-profiler/target/classes",
        "wayang-platforms/wayang-duckdb/target/classes",
        "wayang-platforms/wayang-jdbc-template/target/classes",
        "wayang-platforms/wayang-java/target/classes",
        "wayang-platforms/wayang-spark/target/classes",
        "wayang-platforms/wayang-postgres/target/classes",
        "wayang-platforms/wayang-sqlite3/target/classes",
        "wayang-commons/wayang-core/target/classes",
        "wayang-commons/wayang-basic/target/classes",
        "wayang-commons/wayang-utils-profile-db/target/classes"
    )) {
        Add-IfExists $classpathItems (Join-Path $root $classesDir)
    }

    $classpathItems.Add($dependencyClasspath)
    $classpath = [string]::Join([System.IO.Path]::PathSeparator, $classpathItems)

    $argsFile = Join-Path (Split-Path -Parent (Resolve-RepoPath $Executions)) "duckdb-ga.args"
    @(
        "-cp",
        $classpath,
        "org.apache.wayang.profiler.log.GeneticOptimizerApp",
        (Resolve-WayangFileUrl $Config),
        (Resolve-RepoPath $Executions)
    ) | Set-Content -Encoding ASCII $argsFile

    & java "@$argsFile" *> (Join-Path $root $Log)
    if ($LASTEXITCODE -ne 0) {
        Get-Content (Join-Path $root $Log) -Tail 80
        throw "DuckDB GA profiler failed with exit code $LASTEXITCODE."
    }

    Write-Host "DuckDB GA profiler completed."
    Write-Host "Log: $Log"
}
finally {
    Pop-Location
}
