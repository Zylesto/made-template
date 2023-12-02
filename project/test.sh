#!/bin/bash
python pipeline.py

data_file="../data/zylesto.sqlite"

is_valid_sqlite_file() {

    if [ ! -f "$data_file" ]; then
        echo "Error: The specified path '$data_file' does not exist or is not a regular file path."
        return 1
    elif [ ! -r "$data_file" ]; then
        echo "Error: The file '$data_file' is not readable."
        return 1
    fi

    local file_header=$(head -c 16 "$data_file")
    if [[ "$file_header" == "SQLite format 3"* ]]; then
        echo "The file $data_file is a valid SQLite database."
        return 0
    else
        echo "The file $data_file is not a valid SQLite database."
        return 1
    fi
}

is_valid_sqlite_file


