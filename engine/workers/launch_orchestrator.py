import time
import subprocess
import os
import sys
import json

# CONFIGURATION (loaded from ../config/config.json)
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "config.json")

def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

config = load_config()
orch = config.get("orchestrator", {})
ORCH_ENABLED = bool(orch.get("enabled", True))

MODEL_BASE = orch.get("model_base")
# Single source of truth: config.models (fallback to orchestrator.target_ids)
TARGET_IDS = config.get("models") or orch.get("target_ids") or [
    "liquid/lfm2.5-1.2b-thinking",
    "liquid/lfm2.5-1.2b-thinking:2"
]
GPU_FLAG = orch.get("gpu", "max")

HEARTBEAT_FILE = "heartbeat.lock"
HEARTBEAT_TIMEOUT = orch.get("heartbeat_timeout", 120)
CHECK_INTERVAL = orch.get("check_interval", 5)

def run_lms_command(cmd, wait_s=2):
    """Runs an LMS command via shell and waits."""
    # print(f"   EXEC: {cmd}")
    try:
        startupinfo = None
        if os.name == 'nt':
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW

        # Run with output visible for debugging
        subprocess.run(
            cmd, 
            shell=True,
            startupinfo=startupinfo
        )
        time.sleep(wait_s)
    except Exception as e:
        print(f"   CMD Failing: {e}")

def get_running_identifiers():
    """Parses 'lms ps' to find running model identifiers."""
    running = []
    try:
        startupinfo = None
        if os.name == 'nt':
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW

        result = subprocess.run(
            "lms ps", 
            shell=True, 
            capture_output=True, 
            text=True, 
            encoding='utf-8', 
            startupinfo=startupinfo
        )
        if result.returncode == 0:
            lines = result.stdout.splitlines()
            # Skip header (IDENTIFIER ...)
            for line in lines:
                parts = line.split()
                if len(parts) > 0 and parts[0] != "IDENTIFIER":
                    running.append(parts[0])
    except Exception as e:
        print(f"Error checking status: {e}")
    return running

def restart_all_fresh():
    print("\n🧹  CLEANUP: Unloading ALL models to start fresh...")
    run_lms_command("lms unload --all", wait_s=5)
    
    print("🚀  LAUNCH: Loading Instance 1 (Base)...")
    # Base load (CLI auto-assigns base ID if free, or we force it to be safe)
    # Using explicit identifier ensures we get exactly what we expect
    gpu_arg = f" --gpu={GPU_FLAG}" if GPU_FLAG else ""
    run_lms_command(f"lms load {MODEL_BASE} --identifier {TARGET_IDS[0]}{gpu_arg}", wait_s=5)
    
    print("🚀  LAUNCH: Loading Instance 2 (:2)...")
    run_lms_command(f"lms load {MODEL_BASE} --identifier {TARGET_IDS[1]}{gpu_arg}", wait_s=5)
    
    # Reset heartbeat so we don't loop immediately
    touch_heartbeat()

def touch_heartbeat():
     with open(HEARTBEAT_FILE, "w") as f:
        f.write(str(time.time()))

def main():
    if not ORCH_ENABLED:
        print("🛡️  CLI Orchestrator Disabled (config.orchestrator.enabled=false).")
        return
    if not MODEL_BASE:
        print("⚠️  Orchestrator requires orchestrator.model_base. Exiting.")
        return
    print("🛡️  CLI Orchestrator Active.")
    print(f"   Managing: {TARGET_IDS}")
    
    # 1. Start clean immediately
    restart_all_fresh()
    
    while True:
        try:
            # 2. Check currently running models
            running_ids = get_running_identifiers()
            
            # 3. Check for specific missing instances
            for target_id in TARGET_IDS:
                if target_id not in running_ids:
                    print(f"🔻 Missing {target_id}. Attempting restore...")
                    
                    # Safety unload of this specific ID (kills zombies)
                    run_lms_command(f"lms unload {target_id}", wait_s=2)
                    
                    # Load with explicit identifier to fill the slot for the worker
                    gpu_arg = f" --gpu={GPU_FLAG}" if GPU_FLAG else ""
                    run_lms_command(f"lms load {MODEL_BASE} --identifier {target_id}{gpu_arg}", wait_s=5)
                    
                    # Verify
                    current = get_running_identifiers()
                    if target_id in current:
                        print(f"✅ Restored {target_id}")
                    else:
                        print(f"⚠️ Failed to restore {target_id} (Check logs/resources)")

            # 4. Heartbeat Monitor
            if os.path.exists(HEARTBEAT_FILE):
                try:
                    with open(HEARTBEAT_FILE, "r") as f:
                        content = f.read().strip()
                        if content:
                            last_beat = float(content)
                            age = time.time() - last_beat
                            if age > HEARTBEAT_TIMEOUT:
                                print(f"💔 Heartbeat stopped ({int(age)}s ago). performing FULL RESET.")
                                restart_all_fresh()
                except ValueError:
                    pass
                except Exception:
                    pass
            
            time.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            print("Stopping...")
            break
        except Exception as e:
            print(f"Orchestrator Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
