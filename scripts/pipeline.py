import os
import subprocess
from datetime import datetime

LOG_FILE = "logs/pipeline_log.txt"

def log(message):
    with open(LOG_FILE, "a") as logf:
        logf.write(f"{datetime.now()} | {message}\n")

def run_stage(stage_name, command):
    log(f"START {stage_name}")
    print(f"\n🚀 Running {stage_name}...")
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        log(f"SUCCESS {stage_name}")
        print(f"✅ {stage_name} completed.")
    else:
        log(f"FAILURE {stage_name}")
        print(f"❌ {stage_name} failed.")

def main():
    os.makedirs("logs", exist_ok=True)

    stages = [
        ("Extraction", "python3 scripts/extract.py"),
        ("Transformation", "python3 scripts/transform.py"),
        ("Loading", "python3 scripts/load.py"),
        ("Delivery", "python3 scripts/deliver.py"),
    ]

    for name, cmd in stages:
        run_stage(name, cmd)

    log("PIPELINE RUN COMPLETED")