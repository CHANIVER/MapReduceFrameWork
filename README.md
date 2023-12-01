# MapReduceFramework

**Version**: 1.0.0 (2023.12.01)  
**Project Start Date**: 2023.11.08  
**Authors**: Kwon Yechan, Park Solji, Yun Hongmin

## Project Description

This project is a framework that implements the MapReduce algorithm in C++. Users can define map and reduce functions to perform data analysis tasks that can be parallelized.

## How to Execute

1. **Build**: `g++ -std=c++17 MapReduceFramework.cpp -o MapReduceFramework`
2. **Execution**: `./MapReduceFramework <inputfile> <split_buffer_size> <mapreduce_buffer_size>`
3. **Condition**: The directories 'input', 'output', 'mapout', 'partition', 'sorted', and 'reduceout' must exist in the directory.

## Argument Description

- `<inputfile>`: Path of the input file
- `<split_buffer_size>`: Size of the split buffer
- `<mapreduce_buffer_size>`: Size of the MapReduce buffer

## Development Environment

- Development Environment: Visual Studio Code
- Operating System: macOS
- Programming Language: C++17

## Execution Support Environment

- macOS is recommended. However, it can be executed on any platform that supports C++17.

## Dataset Support Information

- Alphabet: Supported
- Korean: Partially Supported
- Other: Partially Supported

## Key Type Support Information

- string: Supported
- int: Partially Supported
- float: Partially Supported

## Value Type Support Information

- int: Supported
- float: Supported
- string: Partially Supported
