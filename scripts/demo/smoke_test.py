"""Run the canonical migration-aware end-to-end verifier; no alternate pipeline."""
from orchestrate import Orchestrator, arguments

if __name__ == "__main__":
    import sys
    options = sys.argv[1:]
    if "--fast-verify" not in options:
        options = ["--fast-verify", *options]
    raise SystemExit(Orchestrator(arguments(options)).run())
