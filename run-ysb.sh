#!/bin/bash
OPTS="-server -XX:+UseConcMarkSweepGC -XX:NewRatio=2 -XX:SurvivorRatio=16 -Xms8g -Xmx8g"
JCP=".:./BriskBenchmarks:./BriskBenchmarks/target/classes"
JCP="${JCP}:BriskBenchmarks/target/BriskBenchmarks-1.2.0-jar-with-dependencies.jar"
CLASS="applications.BriskRunner"
AGENT="-javaagent:$HOME/briskstream/common/lib/classmexer.jar"
java $AGENT $OPTS -cp $JCP $CLASS --app YSB --compressRatio -1 --backPressure $@
