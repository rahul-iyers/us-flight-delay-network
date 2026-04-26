import sys
import time
import importlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

STEPS = ["01_load_clean", "02_aggregate", "03_network", "04_propagation"]
STEP_NAMES = ["Load & Clean", "Aggregate Stats", "Build Network", "Delay Propagation"]

def run_step(module_name: str) -> None:
    mod = importlib.import_module(module_name)
    mod.main()


def main():
    for i, (step, name) in enumerate(zip(STEPS, STEP_NAMES)):
        print("=" * 60)
        print(f"Step {i + 1}/{len(STEPS)}: {name}")
        print("=" * 60)
        start_time = time.time()
        run_step(step)
        elapsed = time.time() - start_time
        print(f"\nStep {i + 1} completed in {elapsed:.2f} seconds.\n")


if __name__ == "__main__":
    main()
