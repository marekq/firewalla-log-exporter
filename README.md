firewalla-log-exporter
======================

A Lambda function to export Firewalla MSP logs to Axiom. 

# Installation

1. Install AWS SAM locally and set up your AWS credentials.
2. Edit the variables in template.yaml. You can retrieve these from the Firewalla MSP portal and your Axiom account. 
3. Run `sam build` and `sam deploy --guided` to deploy the function to your AWS account.
4. The function will run every 15 minutes and export logs to Axiom.
