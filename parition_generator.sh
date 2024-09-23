#!/bin/bash

# Function to validate date format (YYYY-MM-DD)
validate_date() {
    date -d "$1" "+%Y-%m-%d" &> /dev/null
    if [ $? -ne 0 ]; then
        echo "Invalid date format: $1. Expected format is YYYY-MM-DD."
        exit 1
    fi
}

# Function to generate and execute SQL statements for adding partitions to a Glue table
run_sql_commands() {
    local entity="$1"
    local start_date="$2"
    local end_date="$3"
    local bucket_name="your-bucket-name"  # Update with your actual bucket name
    local glue_table="your_glue_table"    # Update with your actual Glue table name
    local glue_db="your_glue_db"          # Update with your actual Glue database name

    # Validate date formats
    validate_date "$start_date"
    validate_date "$end_date"

    # Convert start and end dates to seconds since the epoch for comparison
    start_date_sec=$(date -d "$start_date" +"%s")
    end_date_sec=$(date -d "$end_date" +"%s")

    # Check if the date range is too large
    date_diff_days=$(( (end_date_sec - start_date_sec) / 86400 ))
    if [ "$date_diff_days" -gt 365 ]; then
        echo "Warning: The date range exceeds one year. Proceeding to create partitions for $date_diff_days days."
    fi

    # Loop through each date in the range
    current_date_sec=$start_date_sec
    while [ "$current_date_sec" -le "$end_date_sec" ]; do
        # Convert seconds to date (YYYY-MM-DD)
        current_date=$(date -d "@$current_date_sec" "+%Y-%m-%d")

        # Loop through numbers 00 to 99
        for number in $(seq -w 0 99); do
            # Construct folder path
            folder="entity=$entity/dt=$current_date/number=$number"

            # Construct the S3 path and the SQL command
            s3_path="s3://$bucket_name/$folder"
            sql_cmd="ALTER TABLE $glue_db.$glue_table
            ADD IF NOT EXISTS PARTITION (entity='$entity', dt='$current_date', number='$number')
            LOCATION '$s3_path';"

            # Print command for reference and execution log
            echo "Executing: $sql_cmd"

            # Execute the SQL command using Spark SQL (or another preferred method)
            # Ensure your environment can run Spark SQL commands
            spark-sql -e "$sql_cmd"

        done

        # Move to the next day
        current_date_sec=$((current_date_sec + 86400))  # Add 86400 seconds (1 day)
    done
}

# Check if the correct number of arguments are passed
if [ $# -ne 3 ]; then
    echo "Usage: $0 <entity> <start_date> <end_date>"
    echo "Example: $0 d1 2024-09-01 2024-09-30"
    exit 1
fi

# Get arguments
entity="$1"
start_date="$2"
end_date="$3"

# Call the function to run the SQL commands with date range filtering
run_sql_commands "$entity" "$start_date" "$end_date"

echo "SQL commands for entity '$entity' from $start_date to $end_date have been executed."
