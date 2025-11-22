# Home Assignment - PlaxidityX

## Directory Structure
- `mysql` - MySQL setup related files 
- `spark` - Spark code files
- `hadoop` - Some Hadoop binaries needed for Spark to work
- `out` - Output CSV files

## Workflow Overview
1. Provisioned a MySQL instance locally using Docker
2. Loaded all data
3. Installed Spark 3.5.7 locally
4. Developed in a Jupyter notebook for faster development
5. Organized the working notebook code into Python files
6. Made sure everything works and organized the directory structure, and some documentation

## NOTES
- MySQL and Spark were used locally as it is simpler and as the data is small enough.
  For production scale a cluster would be provisioned, and some changes will apply to the code,
  e.g better partitioning, caching, optimized JOINs etc.
- Some of the questions were not clear, especially #3. In a real scenario of course I would ask to clarify,
  but here it seemed unnecessary.
- Did not have an S3 bucket available so saved locally.
  Saving to S3 would be a simple change, giving credentials and using boto3.
- Ran locally using Python, ideally would use spark-submit to run on a cluster.
- Did not have time to add full tests, so added only one for example.