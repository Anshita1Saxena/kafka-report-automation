# kafka-report-automation
The purpose of this project is to record the lag and logsize of Kafka topics and send the report via email or on slack. 

## Description
<div align='justify'>
This project is used for automating the task of collection of Kafka metrics and sending them to higher management. Kafka lag metrics show the number of messages stuck in the queue or the messages that are unable to push to the next queue. Kafka logsize metrics show the number of incoming messages that need to be processed. Each message is allotted an offset. This process runs on 13 different types and on 4 different layers:

1. Ingest Layer
2. Persist Layer
3. Pre-Transmit
4. Transmit

The flow of this process is as follows:
1. Collects the lag and logsize from Kafka Topic based on Topic and its partition.
2. Organize the metrics in reports
3. Send the reports by email to specific contacts.

</div>

## Advantages
1. To cut down the efforts for logging into the server, creating reports, and sending these reports to higher management.
2. Reduced 40 hours per week for resources working on this requirement.
3. Reduced time consumed in this activity.

## Environment Details
This project is running on RHEL (Red Hat Enterprize Linux) server. The required packages are listed in `requirements.txt` file. The command to install the required packages is given below:

`pip3 install -r requirements.txt`

The `input` directory holds the files that will be utilized by the code to read the email addresses and the email template. 

The `output` directory holds several directories according to the layers:

1. Type 1 Directory: This type consists of a directory that holds Kafka queue metrics for lag and logsize. These text files are created firstly as an intermediate result.

    intermediate_queue_reports

2. Type 2 Directories: This type consists of several directories holds reports which will be collected by each message type. These reports are created after intermidiate result text files to organize the metrics.

    lag_report_main_ingest, lag_report_main_metricsdb, lag_report_main_persist_retry, lag_report_main_pre-transmit_retry, lag_report_main_transmit, lag_report_main_transmit_retry, lag_report_transmit

3. Type 3 Directories: This type consists of several directories holds reports which will be sent via email and on slack and all reports are the combined result of each message type. The reports kept in this directory is the main set that is created in the end.

    combined_reports_ingest, combined_reports_metricsdb, combined_reports_pre-transmit_retry, combined_reports_transmit, combined_reports_transmit_retry

`conf` directory holds the parameter configuration file.
This project is configuration-based, i.e., in the future if there is any requirement to add the topics for future message types that can be done easily by just mentioning the topic name in the parameter file.

Entry point script is `lag_report.py` which calls `ExcelReportGenerateMultipleQueues.py` for organizing metrics into an excel sheet and `CombinedExcelReportQueues.py` for consolidating all the different message type reports for each layer into one report.

## Working Details
<div align='justify'>
This is configured as a job in crontab which is scheduled to run every hour for the collection of metrics, is used to deliver reports every day via email, and is used to upload the excel sheets to Slack every 2 hours.

To run this code:

`python kafka-report-automation/lag_report.py kafka-report-automation/conf/lag_report.ini`

This project runs in Production. The data which is provided here is modified to maintain confidentiality.
</div>

## Highlights
1. Integration with Kafka.
2. Integration with Slack and Email.
3. Organization of information via Excel sheets.

## Demo Screenshots
1. Reports Uploaded on Slack:
![Slack Screenshot](https://github.com/Anshita1Saxena/kafka-report-automation/blob/main/demo-image/Slack%20Screenshot.JPG)
2. Reports sent via Mail:
![Email Screenshot](https://github.com/Anshita1Saxena/kafka-report-automation/blob/main/demo-image/Mail%20Screenshot.JPG)
3. Report Screenshot:
![Report Screenshot](https://github.com/Anshita1Saxena/kafka-report-automation/blob/main/demo-image/Report%20Snapshot.JPG)
