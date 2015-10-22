# tika-lambda
Run Apache Tika as a service in AWS Lambda by scanning documents in S3 and storing the extracted text back to S3

Heavily based on the work in https://github.com/gnethercutt/tika-lambda

Main changes from the original version:
 - TikaLambdaHandler is no longer abstract. It saves the tika-extracted text back to the S3 bucket with the same key + ".extract"
 - More logging
 - Maven: Upgraded to use aws-lambda-java* v1.1 from v1.0
 - Maven: Locked in to use the aws-java-sdk 1.10.27 due to incompatabilities with what's currently running on AWS Lambda (odd errors about Apache httpcore due to mis-matched versions in the AWS Lambda classpath).  By specifying which version of AWS Java SDK we're on, we can force the classpath to have the version we want and avoid the runtime mis-match errors
