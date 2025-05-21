#!/bin/bash
# Quick test script for scraping coordinator performance testing

# Make sure we're in the data-universe directory
cd "$(dirname "$0")" || exit 1

# Create results directory if it doesn't exist
mkdir -p benchmark_results

# Print banner
echo "======================================================"
echo "   Scraping Coordinator Performance Testing Suite"
echo "======================================================"
echo 

# Check for required packages
check_dependencies() {
    echo "Checking dependencies..."
    python -c "import asyncio, matplotlib, tabulate" 2>/dev/null || {
        echo "Installing required dependencies..."
        pip install matplotlib tabulate
    }
}

# Function to run a test with specified options
run_test() {
    echo "Running test: $1"
    python test_coordinator.py "$@"
}

# Function to run a benchmark with specified options
run_benchmark() {
    echo "Running benchmark: $1"
    python benchmark.py "$@"
}

# Check command line arguments
if [ $# -eq 0 ]; then
    echo "Usage: $0 [quick|workers|batching|all|benchmark|help]"
    echo
    echo "Commands:"
    echo "  quick     - Run a quick single test (30 seconds)"
    echo "  workers   - Test different worker counts"
    echo "  batching  - Test batching vs non-batching"
    echo "  all       - Run all tests"
    echo "  benchmark - Run comprehensive benchmarks"
    echo "  help      - Show this help message"
    exit 1
fi

# Main execution
check_dependencies

case "$1" in
    quick)
        echo "Running quick test (30 seconds)..."
        run_test --test single --duration 30 --workers 10
        ;;
    workers)
        echo "Testing different worker counts..."
        run_test --test workers --duration 20
        ;;
    batching)
        echo "Testing batching vs non-batching..."
        run_test --test batching --duration 30 --workers 10
        ;;
    all)
        echo "Running all tests..."
        run_test --test all
        ;;
    benchmark)
        echo "Running comprehensive benchmarks..."
        run_benchmark --type all --duration 20
        ;;
    help)
        echo "Usage: $0 [quick|workers|batching|all|benchmark|help]"
        echo
        echo "Commands:"
        echo "  quick     - Run a quick single test (30 seconds)"
        echo "  workers   - Test different worker counts"
        echo "  batching  - Test batching vs non-batching"
        echo "  all       - Run all tests"
        echo "  benchmark - Run comprehensive benchmarks"
        echo "  help      - Show this help message"
        ;;
    *)
        echo "Unknown command: $1"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac

echo
echo "Done!"