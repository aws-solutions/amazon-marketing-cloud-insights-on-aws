# Amazon Marketing Cloud Insights on AWS Data Lake

The Amazon Marketing Cloud (AMC) Insights Data Lake is a platform that 
leverages the [Serverless Data Lake Framework](https://github.com/awslabs/aws-serverless-data-lake-framework/) (SDLF) to accelerate the delivery of enterprise data lakes on AWS.

**Purpose**

The Purpose of the AMC Insights Data Lake Framework is to shorten the deployment time to production from several months to a few weeks. Refactoring SDLF on top of the AWS DDK framework allows for more flexible use cases, further customization, and higher level abstraction to reduce the learning curve and make data lakes easier to deploy.

**Integrates with the Tenant Provisioning Service (TPS)**

- Adding or updating a customer record to the Tenant Provisioning Service will trigger automated deployment of a new S3 bucket and a new EventBridge rule that triggers the ETL processes of the data lake for that new bucket source.

**Integrates with the Workflow Management Service (WFM)**

- Scheduling new Workflows will return execution results automatically to the tenant's S3 bucket for further processing in the Data Lake.

**Contents:**

- [Reference Architecture](#reference-architecture)
- [Prerequisites](#prerequisites)
- [AWS Service Requirements](#aws-service-requirements)
- [Resources Deployed](#resources-deployed)
- [Parameters](#parameters)
- [Deployment](#deployment)

## Reference Architecture

![Alt](TODO)

## Prerequisites

* [AWS Command Line Interface](https://aws.amazon.com/cli/)
* [Python](https://www.python.org/) 3.9 or newer
* [Node.js](https://nodejs.org/en/) 16.x or newer
* [AWS CDK](https://aws.amazon.com/cdk/) 2.60.0 or newer
* IDE for e.g. [Pycharm](https://www.jetbrains.com/pycharm/) or [AWS Cloud9](https://aws.amazon.com/cloud9/)

## AWS Service Requirements

The following AWS services are required for this utility:

1. [AWS Lambda](https://aws.amazon.com/lambda/)
2. [Amazon S3](https://aws.amazon.com/s3/)
3. [AWS Glue](https://aws.amazon.com/glue/)
4. [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
5. [AWS Identity and Access Management (IAM)](https://aws.amazon.com/iam/)
6. [Amazon Simple Queue Service](https://aws.amazon.com/sqs/)
7. [AWS Key Management Service (KMS)](https://aws.amazon.com/kms/)
8. [AWS Lake Formation](https://aws.amazon.com/lake-formation/)
9. [AWS EventBridge](https://aws.amazon.com/eventbridge/)
10. [AWS Step Functions](https://aws.amazon.com/step-functions/)

## Resources Deployed

This CDK Application deploys the following resources, separated into dataset and pipeline constructs:

### Datasets

A dataset is a logical construct referring to a grouping of data. It can be anything from a single table to an entire database with multiple tables, for example. However, an overall good practice is to limit the infrastructure deployed to the minimum to avoid unnecessary overhead and cost. It means that in general, the more data is grouped together the better. Abstraction at the transformation code level can then help make distinctions within a given dataset.

Examples of datasets are:

- A relational database with multiple tables (E.g. Sales DB with orders and customers tables)
- A group of files from a data source (E.g. XML files from a Telemetry system)
- A streaming data source (E.g. Kinesis data stream batching files and dumping them into S3)

For this QuickStart, a Glue database and crawler alongside Lake Formation permissions are created. However, this will depend on the use case with the requirements for unstructured data or for a streaming data source likely to be different.

### Pipelines

A data pipelines can be thought of as a logical constructs representing an ETL process that moves and transforms data from one area of the lake to another. The stages directory is where the blueprint for a data pipeline stage is defined by data engineers. For instance, the definition for a step function stage orchestrating a Glue job and updating metadata is abstracted in the `sdlf_heavy_transform.py` file. This definition is not specific to a particular job or crawler, instead the Glue job name is passed as an input to the stage. Such a configuration promotes the reusability of stages across multiple data pipelines.

In the pipelines directory, these stage blueprints are instantiated and wired together to create a data pipeline. Borrowing an analogy from object-oriented programming, blueprints defined in the stages directory are “classes”, while in the pipelines directory these become “object instances” of the class.

## Parameters

1. `team` — The name of the team which owns the pipeline.
2. `pipeline` — Name to give the pipeline being deployed.
3. `dataset` - Name of the dataset being deployed.

The `datasets_parameters.json` is the same level as `app.py` within `infrastructure`. If the parameters are not filled, default values for team, pipeline, and dataset will be used (i.e. demoteam, adtech, and amcdataset).

### Deployment

Refer to the AMC Insights Deployment Steps.
