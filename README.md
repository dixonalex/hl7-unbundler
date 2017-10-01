# hl7-unbundler

Simple pipeline for converting a normalized data structure into a tabular format for batch processing.

## Solution Notes

This solution takes a simple approach to parsing the example input file. The following are some key areas for improvement:

* Reads the full file into memory for processing.
* Makes no assumption of incoming or final data structure. The tabular structure is calculated by iterating over the JSON keys. This could lead to final data structures that are unexpected or unwanted.

## How To use

The Docker image is meant to be used with the awslabs CloudFormation template running on an EC2 instance. Check out the writeup in [this repo](https://github.com/awslabs/ecs-refarch-batch-processing).