#!/bin/bash

# =========================================================
# AUTO-SCHEDULER FOR ETL PIPELINE
# =========================================================

# Detect Project Paths
PROJECT_DIR=$(pwd)
PYTHON_EXEC="$PROJECT_DIR/envp310/bin/python3" 
LOG_FILE="$PROJECT_DIR/logs/cron_job.log"

# Validation
if [ ! -f "$PYTHON_EXEC" ]; then
    echo "Error: Python executable not found at $PYTHON_EXEC"
    echo "   Please ensure you created the virtual environment 'envp310'"
    exit 1
fi

# Ensure log directory exists
mkdir -p "$PROJECT_DIR/logs"

# Define the Job (Runs Daily at 06:00 AM)
CRON_JOB="0 6 * * * $PYTHON_EXEC $PROJECT_DIR/main.py >> $LOG_FILE 2>&1"

(crontab -l 2>/dev/null | grep -v -F "$PROJECT_DIR/main.py"; echo "$CRON_JOB") | crontab -

# Success Message
echo "Success! Pipeline scheduled."
echo "Frequency: Daily at 06:00 AM"
echo "Log File:  $LOG_FILE"
echo "Verify:    Run 'crontab -l' to see the job."