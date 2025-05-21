import asyncio
import json
import os
import datetime as dt
import matplotlib.pyplot as plt
from tabulate import tabulate
import numpy as np
from test_coordinator import run_test, test_worker_scaling, test_batching, run_all_tests

class Benchmark:
    def __init__(self, results_dir="benchmark_results"):
        self.results_dir = results_dir
        os.makedirs(results_dir, exist_ok=True)
        self.timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        
    async def run_single_benchmark(self, name, workers=10, duration=60, batching=False):
        """Run a single benchmark with specified parameters"""
        print(f"\n=== Running benchmark: {name} ===")
        result = await run_test(
            duration_seconds=duration,
            worker_count=workers,
            use_batching=batching
        )
        
        result["name"] = name
        result["config"] = {
            "workers": workers,
            "duration": duration,
            "batching": batching
        }
        
        return result
    
    async def run_standard_benchmarks(self, duration=60):
        """Run a standard set of benchmarks"""
        configs = [
            {"name": "baseline", "workers": 5, "batching": False},
            {"name": "more_workers", "workers": 20, "batching": False},
            {"name": "batching", "workers": 5, "batching": True},
            {"name": "optimized", "workers": 20, "batching": True},
        ]
        
        results = {}
        for config in configs:
            result = await self.run_single_benchmark(
                config["name"], 
                workers=config["workers"], 
                duration=duration,
                batching=config["batching"]
            )
            results[config["name"]] = result
            
        return results
    
    async def run_worker_scaling_benchmark(self, duration_per_test=30):
        """Run worker scaling benchmarks"""
        print("\n=== Running worker scaling benchmark ===")
        results = await test_worker_scaling(duration_per_test=duration_per_test)
        return {
            "type": "worker_scaling",
            "duration_per_test": duration_per_test,
            "results": [{"workers": w, **r} for w, r in results]
        }
    
    async def run_batching_benchmark(self, duration_per_test=60, worker_count=10):
        """Run batching comparison benchmark"""
        print("\n=== Running batching benchmark ===")
        results = await test_batching(
            duration_per_test=duration_per_test,
            worker_count=worker_count
        )
        return {
            "type": "batching_comparison",
            "duration_per_test": duration_per_test,
            "worker_count": worker_count,
            "results": results
        }
    
    def save_results(self, results, name=None):
        """Save benchmark results to a file"""
        if name is None:
            name = "benchmark"
        
        filename = f"{name}_{self.timestamp}.json"
        filepath = os.path.join(self.results_dir, filename)
        
        with open(filepath, "w") as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\nResults saved to {filepath}")
        return filepath
    
    def generate_report(self, results, report_type="standard"):
        """Generate a report from benchmark results"""
        if report_type == "standard":
            # Create a table of results
            headers = ["Benchmark", "Workers", "Batching", "Scrapes/s", "Entities/s", "Writes/s"]
            rows = []
            
            for name, result in results.items():
                rows.append([
                    name,
                    result["worker_count"],
                    "Yes" if result["batching_enabled"] else "No",
                    f"{result['scrapes_per_second']:.2f}",
                    f"{result['entities_per_second']:.2f}",
                    f"{result['storage_writes_per_second']:.2f}"
                ])
            
            print("\n=== Benchmark Report ===")
            print(tabulate(rows, headers=headers, tablefmt="pipe"))
            
            # Generate plots if matplotlib is available
            try:
                self._generate_plots(results)
            except ImportError:
                print("matplotlib not available for plotting")
    
    def _generate_plots(self, results):
        """Generate plots from benchmark results"""
        # Extract data for plotting
        names = list(results.keys())
        scrapes_per_second = [r["scrapes_per_second"] for r in results.values()]
        entities_per_second = [r["entities_per_second"] for r in results.values()]
        
        # Create plot
        fig, ax = plt.subplots(1, 2, figsize=(12, 5))
        
        # Plot scrapes per second
        ax[0].bar(names, scrapes_per_second)
        ax[0].set_title("Scrapes per Second")
        ax[0].set_ylabel("Scrapes/s")
        ax[0].grid(axis="y", linestyle="--", alpha=0.7)
        
        # Plot entities per second
        ax[1].bar(names, entities_per_second)
        ax[1].set_title("Entities per Second")
        ax[1].set_ylabel("Entities/s") 
        ax[1].grid(axis="y", linestyle="--", alpha=0.7)
        
        plt.tight_layout()
        plot_path = os.path.join(self.results_dir, f"benchmark_plot_{self.timestamp}.png")
        plt.savefig(plot_path)
        print(f"Plot saved to {plot_path}")

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run benchmarks for the scraping coordinator")
    parser.add_argument("--type", choices=["standard", "worker_scaling", "batching", "all"], 
                      default="standard", help="Type of benchmark to run")
    parser.add_argument("--duration", type=int, default=60, 
                      help="Duration of each benchmark in seconds")
    parser.add_argument("--workers", type=int, default=10, 
                      help="Number of workers for batching benchmark")
    args = parser.parse_args()
    
    benchmark = Benchmark()
    
    if args.type == "standard":
        results = await benchmark.run_standard_benchmarks(duration=args.duration)
        benchmark.save_results(results, "standard_benchmark")
        benchmark.generate_report(results)
    
    elif args.type == "worker_scaling":
        results = await benchmark.run_worker_scaling_benchmark(duration_per_test=args.duration)
        benchmark.save_results(results, "worker_scaling")
    
    elif args.type == "batching":
        results = await benchmark.run_batching_benchmark(
            duration_per_test=args.duration,
            worker_count=args.workers
        )
        benchmark.save_results(results, "batching_benchmark")
    
    elif args.type == "all":
        standard_results = await benchmark.run_standard_benchmarks(duration=30)
        benchmark.save_results(standard_results, "standard_benchmark")
        benchmark.generate_report(standard_results)
        
        worker_results = await benchmark.run_worker_scaling_benchmark(duration_per_test=20)
        benchmark.save_results(worker_results, "worker_scaling")
        
        batching_results = await benchmark.run_batching_benchmark(duration_per_test=30)
        benchmark.save_results(batching_results, "batching_benchmark")

if __name__ == "__main__":
    asyncio.run(main())