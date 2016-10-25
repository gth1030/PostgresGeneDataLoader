#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "$@"
java -cp "$DIR"/target/classes:"$DIR"/src/main/resources/* BedFileConvertor "$@"
