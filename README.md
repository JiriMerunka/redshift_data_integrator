# Redshift Integrator

## Redshift Integrator

- loads csv files from S3 to AWS Redshift Instance
- generates SQL (DDL,DL,COPY COMMANDS) by using Liquid Template language + config files in JSON (1 global = config.json + multiple in param_definitions dir - config for each table) and executes them in Redshift
