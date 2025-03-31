# HTCondor Simulation for School Registry Data

This project simulates an HTCondor-like distributed computing environment for processing school registry data. It includes scripts to generate random school data (similar to what might be returned from distributed jobs) and to process that data into structured Parquet files with proper relationship handling.

## Overview

The simulation consists of three main components:

1. **JSON Generator** (`json_generator.py`): Simulates distributed computing jobs by generating random school registry data in JSON format at configurable intervals
2. **Result Listener** (`result_listener.py`): Continuously monitors for new JSON files, validates them, and processes them into related Parquet files
3. **Simulation Runner** (`run_simulation.py`): Controls and runs the entire simulation with configurable parameters

## Data Model

The simulation uses a school registry data model with the following entities:

- **Students**: Information about students (id, name, age, gender, year, gpa, contact details, etc.)
- **Teachers**: Information about teachers (id, name, department, contact details, qualifications, etc.)
- **Classes**: Information about classes (id, name, room, teacher, schedule, capacity, etc.)
- **Student-Class Relationships**: Many-to-many relationships between students and classes (enrollment details, grades, etc.)

## Requirements

- Python 3.8+
- Required packages:
  - pandas
  - pyarrow
  - numpy
  - pydantic

Install dependencies:
```bash
pip install pandas pyarrow numpy pydantic
```

## Usage

### Running the Complete Simulation

The easiest way to run the simulation is using the `run_simulation.py` script:

```bash
python run_simulation.py
```

This will:
1. Start generating random school data JSONs in the `./input` directory
2. Start monitoring for those JSONs and processing them into Parquet files in the `./output` directory
3. Continue running until stopped with Ctrl+C or until the specified number of results are generated

### Command-Line Options

The simulation runner supports various options to control the simulation behavior:

```
python run_simulation.py --help
```

Common options:

- `--num-results N`: Generate exactly N JSON results then stop
- `--timeout X`: Maximum simulation runtime in seconds (default: 600s = 10 minutes)
- `--generator-rate X`: Number of JSON files to generate per second (default: 0.33 per second)
- `--max-batch-size N`: Maximum entities per batch (default: 10)
- `--summary`: Display summary of generated Parquet files at the end
- `--performance-metrics`: Display detailed performance statistics (timing, throughput, etc.)
- `--completion-wait X`: Wait X seconds after generator completes to allow listener to finish (default: 5.0)

### Running Components Separately

You can also run the generator and listener separately:

Generate random JSONs:
```bash
python json_generator.py --output-dir ./input --num-iterations 100
```

Listen for JSONs and process them:
```bash
python result_listener.py --input-dir ./input --output-dir ./output
```

## Directory Structure

- `./input`: Directory where JSON files are generated
- `./output`: Directory where Parquet files are saved
- `./processed`: Directory where processed JSON files are moved (if enabled)

## Output Data

The simulation produces four Parquet files:

1. `students.parquet`: Student information
2. `teachers.parquet`: Teacher information
3. `classes.parquet`: Class information
4. `student_classes.parquet`: Many-to-many relationships between students and classes

## Customization

The scripts are designed to be easily customizable:

- Modify the generation parameters to adjust the data size and complexity
- Extend the data models to include additional fields or relationships
- Adjust the processing logic to handle different validation or transformation requirements

## Notes for Real HTCondor Integration

For actual HTCondor integration:

1. The `json_generator.py` would be replaced by your actual HTCondor jobs
2. The job definitions would need to output data in a similar JSON format
3. The `result_listener.py` would remain largely unchanged, just listening for results from actual HTCondor jobs instead of simulated ones

## Troubleshooting

Common issues:

- **Missing directories**: The scripts will create required directories automatically
- **File access errors**: Ensure the process has write permissions to the directories
- **Invalid JSON errors**: If modifying the scripts, ensure JSON data remains valid and matches the expected schema
