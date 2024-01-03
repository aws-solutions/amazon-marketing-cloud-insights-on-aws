# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.2] - 2024-01-03
### Added
- Added timestamps to processed file names for prevention of accidental overwrites
- Fixed null values casting as -1
- Fixed column dropping issue caused by backslash characters in column cell values
- Updated state machine to only trigger on successful file uploads

## [2.0.1] - 2023-10-25
### Added
- Update urllib to v1.26.18
- Fix operational policy permissions

## [2.0.0] - 2023-10-05

### Added

- Promote to AWS Solutions.
- Integrate Microservice 2.0.
- Add Cross-Account and Cross-Region functionality.
- Update cross-account template synthesis and deployment process.
- Enhancements for multi-instance customers.
- Add CloudWatch alarms to DLQs when message count > 0.
- Add CloudWatch alarms to Lambdas for error and throttle metrics.
- Enable CloudTrail for S3 and Lambda data events and logs for Step Functions
- User's ability to optionally deploy either microservices or data lake.
- User's ability to add other data source to the data lake.
- Remove CodeBuild infrastructure and usage of AWS DDK.
- Add functional and unit tests.
- Add AppRegistry support.
- Improve codebase to meet Solution quality bar.
- Reforge and simplify six stacks to one stack. 
- Allow multiple stack deployment in one region. 
- Update folder structure and add files for Solutions layout.
- Incorporate CDK solution helper and update build output.
- Update runtimes, layers, timeouts, architectures, dependencies, package hierarchy, and copyrights.
- Reforge AWS Data Wrangler layer build.
- Update DynamoDB tables to on-demand capacity.
- Update scripts for uninstalling solution.
- Add, update and fix IAM role and policy.
- Restrict KMS permissions. 
- Bug fix and enhancements

## [1.1.3] - 2023-06-01

### Added

- Final version before promotion to AWS Solutions

