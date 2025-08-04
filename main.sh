#!/bin/zsh

# Static directory path
CODE_DIR="/Users/dishen/Documents/dishen/atliq/python/news-data-retrieval"
CRON_LOG_FILE="${CODE_DIR}/main.log"

echo -e "\n\n\n--------------------------------------------- CRON START LOGS ---------------------------------------------\n" >> "$CRON_LOG_FILE"

# Navigate to the directory
if [ -d "$CODE_DIR" ]; then
    cd "$CODE_DIR" || { echo -e "\n\nFailed to navigate to $CODE_DIR" >> "$CRON_LOG_FILE"; exit 1; }
else
    echo -e "Directory $CODE_DIR does not exist!" >> "$CRON_LOG_FILE"
    exit 1
fi

# Log cronjob start time for tracking
echo -e "\n\n$(date '+%Y-%m-%d %H:%M:%S') - Starting run_main.sh" >> "$CRON_LOG_FILE"

# Virtual environment activation script
PYTHON_VENV="${CODE_DIR}/venv/bin/activate"

# Activate the virtual environment
if [ -f "$PYTHON_VENV" ]; then
    echo -e "\n\nActivating virtual environment: $PYTHON_VENV" >> "$CRON_LOG_FILE"
    source "$PYTHON_VENV"
else
    echo -e "\n\nVirtual environment activation script not found at $PYTHON_VENV!" >> "$CRON_LOG_FILE"
    exit 1
fi

# Load environment variables from .env file in CODE_DIR
if [ -f .env ]; then
    # Read .env file, ignore lines with comments or invalid entries
    while IFS='=' read -r key value; do
        # Ignore lines that are comments or blank
        if [[ ! $key =~ ^# ]] && [[ -n $key ]]; then
            export "$key=$value"
        fi
    done < .env
else
    echo -e "\n\n.env file not found in $CODE_DIR!" >> "$CRON_LOG_FILE"
    exit 1
fi

# Define Python scripts and log files in sequence
# declare -A scripts=(
#     ["main.py"]="main.log"
# )
# scripts_to_run=("main.py" "main2.py")

scripts_to_run=("app.py")

# Run the selected Python scripts
for script in "${scripts_to_run[@]}"; do
    script_path="${CODE_DIR}/${script}"

    if [ -f "$script_path" ]; then

        echo -e "Running python file: $script_path" >> "$CRON_LOG_FILE"

        echo -e "\n\n\n --------------------------------------------- CRON START LOGS ---------------------------------------------\n" >> "$CRON_LOG_FILE"
        echo -e "\n\n$(date -u '+%Y-%m-%d %H:%M:%S') - Starting $script" >> "$CRON_LOG_FILE"

        python3 "$script_path" >> "$CRON_LOG_FILE" 2>&1

        echo -e "\n\n$(date -u '+%Y-%m-%d %H:%M:%S') - Finished $script" >> "$CRON_LOG_FILE"
        
        echo -e "\n--------------------------------------------- CRON END LOGS --------------------------------------------- \n\n\n" >> "$CRON_LOG_FILE"
    else
        echo -e "\n\n Script $script_path not found!" >> "$CRON_LOG_FILE"
    fi

done

echo -e "\n\n$(date -u '+%Y-%m-%d %H:%M:%S') - Finished run_main.sh" >> "$CRON_LOG_FILE"
echo -e "\n--------------------------------------------- CRON END LOGS --------------------------------------------- \n\n\n" >> "$CRON_LOG_FILE"