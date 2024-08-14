#!/bin/bash

TARGET_BASE=/Users/kamir/GITHUB.active/incubator-wayang
SOURCE_BASE=/Users/kamir/GITHUB.active/kamir-incubator-wayang

# List of files to copy
FILES_TO_COPY=(
    "wayang-api/wayang-api-scala-java/code/main/scala/org/apache/wayang/api/DataQuanta.scala"
    "wayang-api/wayang-api-scala-java/code/main/scala/org/apache/wayang/api/DataQuantaBuilder.scala"
    "wayang-api/wayang-api-scala-java/code/main/scala/org/apache/wayang/api/JavaPlanBuilder.scala"

#    "wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/KafkaTopicSink.java"
#    "wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/KafkaTopicSource.java"

#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/api/Configuration.java"

#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/function/FunctionDescriptor.java"
#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/function/TransformationDescriptor.java"

#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/ProbabilisticDoubleInterval.java"
#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/ProbabilisticIntervalEstimate.java"

#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/CardinalityEstimate.java"
#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/CardinalityEstimator.java"
#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/cardinality/DefaultCardinalityEstimator.java"

#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/LoadEstimate.java"
#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/optimizer/costs/LoadProfile.java"
#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/InputSlot.java"
#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/OperatorAlternative.java"
#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/OperatorBase.java"
#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/OperatorContainers.java"
#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/Slot.java"
#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/SlotMapping.java"
#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/plan/wayangplan/UnarySink.java"
#
#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/types/BasicDataUnitType.java"
#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/types/DataSetType.java"
#    "wayang-commons/wayang-core/src/main/java/org/apache/wayang/core/types/DataUnitType.java"
#
#    "wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/KafkaTopicSinkMapping.java"
#    "wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/KafkaTopicSourceMapping.java"
#    "wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/mapping/Mappings.java"

#    "wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkKafkaTopicSink.java"
#    "wayang-platforms/wayang-spark/code/main/java/org/apache/wayang/spark/operators/SparkKafkaTopicSource.java"

#    "wayang-platforms/wayang-spark/pom.xml"
#    "wayang-platforms/wayang-spark/wayang-spark_2.12/pom.xml"
#    "wayang-commons/wayang-basic/pom.xml"
)

# Copy each file from SOURCE_BASE to TARGET_BASE
for file in "${FILES_TO_COPY[@]}"; do
    src="${SOURCE_BASE}/${file}"
    dest="${TARGET_BASE}/${file}"

    # Create the destination directory if it does not exist
    # mkdir -p "$(dirname "${dest}")"

    # Copy the file
    cp "${src}" "${dest}"
    echo "SRC: ${src}"
    echo "DST: ${dest}"
    diff ${src} ${dest}

done

echo "All files copied successfully."
