#!/usr/bin/env python3
"""
Test runner for jpy-sync-db-lite with conditional execution.

This script allows you to run different categories of tests based on your needs.

Usage:
    python tests/run_tests.py [options]

Options:
    --fast, -f          Run only fast unit tests (default)
    --full, -a          Run all tests
    --core, -c          Run core functionality tests only
    --batch, -b         Run batch operation tests
    --edge, -e          Run edge case tests
    --sqlite, -s        Run SQLite-specific tests
    --coverage, -v      Run coverage tests
    --performance, -p   Run performance tests
    --integration, -i   Run integration tests
    --slow, -l          Run slow tests
    --help, -h          Show this help message

Examples:
    python tests/run_tests.py --fast          # Run only fast unit tests
    python tests/run_tests.py --core          # Run core functionality tests
    python tests/run_tests.py --batch --edge  # Run batch and edge case tests
    python tests/run_tests.py --full          # Run all tests
"""

import argparse
import os
import sys
import subprocess
import time
from pathlib import Path


def run_tests(test_files, markers=None, verbose=True):
    """Run tests using pytest."""
    cmd = [sys.executable, "-m", "pytest"]
    
    if verbose:
        cmd.append("-v")
    
    # Add markers if specified
    if markers:
        for marker in markers:
            cmd.extend(["-m", marker])
    
    # Add test files
    cmd.extend(test_files)
    
    # Add coverage options
    cmd.extend([
        "--cov=jpy_sync_db_lite",
        "--cov-report=term-missing",
        "--cov-report=html",
        "--cov-fail-under=80"
    ])
    
    if verbose:
        print(f"Running command: {' '.join(cmd)}")
        print("-" * 80)
    
    start_time = time.time()
    result = subprocess.run(cmd, cwd=Path(__file__).parent.parent)
    end_time = time.time()
    
    if verbose:
        print("-" * 80)
        print(f"Tests completed in {end_time - start_time:.2f} seconds")
    
    return result.returncode


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Test runner for jpy-sync-db-lite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Test category options
    parser.add_argument("--fast", "-f", action="store_true", 
                       help="Run only fast unit tests (default)")
    parser.add_argument("--full", "-a", action="store_true", 
                       help="Run all tests")
    parser.add_argument("--core", "-c", action="store_true", 
                       help="Run core functionality tests only")
    parser.add_argument("--batch", "-b", action="store_true", 
                       help="Run batch operation tests")
    parser.add_argument("--edge", "-e", action="store_true", 
                       help="Run edge case tests")
    parser.add_argument("--sqlite", "-s", action="store_true", 
                       help="Run SQLite-specific tests")
    parser.add_argument("--coverage", "-v", action="store_true", 
                       help="Run coverage tests")
    parser.add_argument("--performance", "-p", action="store_true", 
                       help="Run performance tests")
    parser.add_argument("--integration", "-i", action="store_true", 
                       help="Run integration tests")
    parser.add_argument("--slow", "-l", action="store_true", 
                       help="Run slow tests")
    
    # Other options
    parser.add_argument("--quiet", "-q", action="store_true", 
                       help="Run tests quietly (less output)")
    
    args = parser.parse_args()
    
    # Determine which tests to run
    test_files = []
    markers = []
    
    if args.full:
        # Run all test files
        test_files = [
            "tests/test_db_engine.py",
            "tests/test_db_engine_core.py",
            "tests/test_db_engine_batch.py",
            "tests/test_db_engine_edge_cases.py",
            "tests/test_db_engine_sqlite.py",
            "tests/test_db_engine_coverage.py",
            "tests/test_sql_helper.py",
            "tests/test_db_engine_performance.py"
        ]
    elif args.core:
        test_files = ["tests/test_db_engine.py"]
        markers = ["unit"]
    elif args.batch:
        test_files = ["tests/test_db_engine_batch.py"]
    elif args.edge:
        test_files = ["tests/test_db_engine_edge_cases.py"]
    elif args.sqlite:
        test_files = ["tests/test_db_engine_sqlite.py"]
    elif args.coverage:
        test_files = ["tests/test_db_engine_coverage.py"]
    elif args.performance:
        test_files = ["tests/test_db_engine_performance.py"]
    elif args.integration:
        markers = ["integration"]
        test_files = [
            "tests/test_db_engine.py",
            "tests/test_db_engine_core.py",
            "tests/test_db_engine_batch.py",
            "tests/test_db_engine_sqlite.py"
        ]
    elif args.slow:
        markers = ["slow"]
        test_files = [
            "tests/test_db_engine.py",
            "tests/test_db_engine_core.py",
            "tests/test_db_engine_batch.py",
            "tests/test_db_engine_performance.py"
        ]
    else:
        # Default: fast unit tests
        test_files = ["tests/test_db_engine.py"]
        markers = ["unit"]
    
    # Filter out non-existent test files
    existing_files = []
    for test_file in test_files:
        if os.path.exists(test_file):
            existing_files.append(test_file)
        else:
            if not args.quiet:
                print(f"Warning: Test file {test_file} not found, skipping...")
    
    if not existing_files:
        print("Error: No test files found to run!")
        return 1
    
    if not args.quiet:
        print(f"Running tests: {', '.join(existing_files)}")
        if markers:
            print(f"With markers: {', '.join(markers)}")
        print()
    
    # Run the tests
    return run_tests(existing_files, markers, verbose=not args.quiet)


if __name__ == "__main__":
    sys.exit(main())
