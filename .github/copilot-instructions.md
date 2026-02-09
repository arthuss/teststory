# Project Structure & Organization Rules

## Directory Structure
Maintain the following file organization strictly:

- **Python Scripts (`.py`)**: MUST start in `engine/workers/`.
  - Example: `engine/workers/run_stage.py`
  - Example: `engine/workers/init_structure.py`

- **PowerShell Scripts (`.ps1`)**: MUST be placed in `engine/scripts/`.

- **Configuration (`.json`, `.ini`)**: MUST be placed in `engine/config/`.
  - The main config is `engine/config/config.json`.

- **Input Data**: Source texts and instructions go in `input/`.
  - Example: `input/complete_story.txt`

- **Backups**: Old data files go in `backups/`.

- **Root Directory**: Keep the root clean. Only essential files like `README.md`, `.gitignore`, and the active `story_data.json` should remain here.

## Path References
When writing scripts in `engine/workers/`:
- Config is located at `../config/config.json`.
- Data file is usually at `../../story_data.json`.
- Input files are at `../../input/`.
- Always use relative paths or `os.path.join` with `os.path.dirname(__file__)` to ensure portability.
