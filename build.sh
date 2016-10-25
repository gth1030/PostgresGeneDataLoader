#!/bin/bash

cd "$( dirname "${BASH_SOURCE[0]}" )"
mkdir -p target/classes
javac -d "target/classes" -cp "src/main/resources/*" src/main/java/*.java
