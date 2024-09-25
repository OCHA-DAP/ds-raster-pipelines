# Contributing

See the following checklist for steps to implement a new pipeline:

## New pipeline checklist

- Create a config file, `<new_pipeline>_config.yml` with pipeline-specific settings in the `src/config/` directory. Follow the schema of existing config files in this directory.
- Create a new file `<new_pipeline>_pipeline.py` in the `src/pipelines/` directory.
  - Implement the new pipeline class, inheriting from the base `Pipeline` class. Implement all required abstract methods.
- Set up a runner script for the pipeline: `src/scripts/run_<new_pipeline>_pipeline.py`. Set up any pipeline-specific input arguments here via `argparse`.
- Update `src/scripts/run_pipeline.py` to include the new pipeline option.
- Write unit tests for the new pipeline in the `tests/` directory.
- Update the main README.md with information about the new pipeline.
- Create example notebooks of dataset usage in `examples/`.
