# Scraping Coordinator Testing Framework

This testing framework allows you to evaluate and optimize the performance of the scraping coordinator without requiring blockchain dependencies. It provides tools to measure scraping rates, database write efficiency, and test different optimization strategies.

## Components

The testing framework includes:

- **mock_scraper.py** - Mock implementation of scrapers with configurable latency
- **mock_storage.py** - Mock implementation of storage with performance metrics
- **mock_bittensor.py** - Mock implementation of bittensor modules
- **test_coordinator.py** - Test harness for running various tests
- **benchmark.py** - Script for running standardized benchmarks and generating reports

## Running Tests

### Basic Single Test

To run a simple test with default parameters:

```bash
python test_coordinator.py
```

This will run a test with 10 workers for 60 seconds using the default configuration.

### Command Line Options

The `test_coordinator.py` script supports several command line options:

```bash
python test_coordinator.py --test [single|workers|batching|all] --duration 60 --workers 10 --batching
```

- `--test`: Type of test to run
  - `single`: Run a single test with specified parameters
  - `workers`: Test different worker counts
  - `batching`: Compare batching vs non-batching
  - `all`: Run all tests
- `--duration`: Test duration in seconds
- `--workers`: Number of workers for single tests
- `--batching`: Enable batching for single test

### Examples

Test with 20 workers for 30 seconds:
```bash
python test_coordinator.py --test single --duration 30 --workers 20
```

Test different worker counts:
```bash
python test_coordinator.py --test workers --duration 20
```

Test batching vs non-batching:
```bash
python test_coordinator.py --test batching --duration 30 --workers 15
```

## Running Benchmarks

For more comprehensive benchmarks and reports, use the `benchmark.py` script:

```bash
python benchmark.py --type [standard|worker_scaling|batching|all] --duration 60 --workers 10
```

- `--type`: Type of benchmark to run
  - `standard`: Run standard set of benchmarks (baseline, more_workers, batching, optimized)
  - `worker_scaling`: Test performance with different worker counts
  - `batching`: Test performance with and without batching
  - `all`: Run all benchmark types
- `--duration`: Duration of each benchmark in seconds
- `--workers`: Number of workers for batching benchmark

### Examples

Run standard benchmarks:
```bash
python benchmark.py --type standard --duration 45
```

Run worker scaling benchmark:
```bash
python benchmark.py --type worker_scaling --duration 20
```

## Understanding Results

Each test will output performance metrics including:

- **Scrapes per second** - Number of scrape operations completed per second
- **Entities per second** - Number of data entities generated per second
- **Storage writes per second** - Number of storage write operations per second
- **Avg Write Time** - Average time spent on storage write operations

Benchmark results are saved to the `benchmark_results` directory as JSON files, and summary reports are displayed.

## Extending the Framework

### Testing New Optimization Strategies

To test a new optimization strategy:

1. Modify the `test_coordinator.py` file to implement your optimization
2. Create a new test function or extend existing ones
3. Update the benchmark script if needed to include your new optimization

### Adding Performance Metrics

To add new performance metrics:

1. Update the mock classes to track additional metrics
2. Modify the result dictionaries to include the new metrics
3. Update the report generation to display the new metrics

## Potential Optimization Strategies to Test

Some strategies you might want to test:

1. **Worker count optimization** - Find the optimal number of workers for your system
2. **Batch processing** - Group entities before writing to storage
3. **Domain-based rate limiting** - Avoid getting blocked by implementing domain-specific rate limits
4. **Prioritized queue** - Process more important scrapes first
5. **Connection pooling** - Reuse connections for better network efficiency

## Next Steps

After identifying the most effective optimization strategies through testing, implement them in your production codebase. Consider implementing critical path components in Go for better concurrency if the test results show that the Python implementation is a bottleneck.