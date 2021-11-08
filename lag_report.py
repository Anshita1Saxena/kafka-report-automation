#!/usr/bin/python

"""
This code generates the excel report for Kafka metrics for various Topics.
"""

__author__ = "Anshita Saxena"
__copyright__ = "(c) Copyright IBM 2019"
__credits__ = ["BAT DMS IBM Team"]
__maintainer__ = "Anshita Saxena"
__email__ = "anshsa33@in.ibm.com"
__status__ = "Production"

# Library for delegating tasks to OS (Operating System)
import subprocess
# Library for SMTP (Simple Mail Transfer Protocol)
import smtplib
from smtplib import SMTPException
from string import Template
# Library for datetime
import datetime
# Library for exception
import traceback
# Library to interact with interpreter
import sys
# HTTP library
import requests
# Custom classes for reports creation
from ExcelReportGenerateMultipleQueues import ExcelReportGenerate
from CombinedExcelReportQueues import CombinedExcelReport
# Library for email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
# Library for configuration language for ini files
import configparser

# A constant variable that consists of sender email address.
SENDEREMAILADD = "xxxxxxx@gmail.com"

CONFIG = configparser.ConfigParser()

"""
A constant variable consists of username with the server ip address.
For example: if 'root' is a username and 'XX.XXX.XXX.XX' as private ip address\
 of server.
Then, root@XX.XXX.XXX.XX should be the HOST.
It logins to the other server from the script running server without the use \
of paramiko library.
"""
HOST = "root@XX.XXX.XXX.XX"


# This function will raise an exception when the expected arguments not passed.
def extract_command_params(arguments):
    if len(arguments) != 2:
        raise Exception('Illegal number of arguments. \
                        Usage: python lag_report.py lag_report.ini')

    APP_CONFIG_FILE = arguments[1]
    return APP_CONFIG_FILE


# This function is used for setup the value of all the variables.
def set_env(APP_CONFIG_FILE):
    """
    Set all Global constants here, from .ini file.
    A Global constant will have a prefix "G_"
    :param APP_CONFIG_FILE:
    :return:

    """

    try:
        # Reading configuration parameters from .ini file.
        CONFIG.read(APP_CONFIG_FILE)

        # Names and Email addresses of people to send report
        global G_CONTACT_INPUT_FILE
        G_CONTACT_INPUT_FILE = CONFIG['ApplicationParams\
                                    ']['contact_input_file']

        # Mail Template
        global G_MESSAGE_INPUT_FILE
        G_MESSAGE_INPUT_FILE = CONFIG['ApplicationParams\
                                    ']['message_input_file']

        # Names and Email addresses of people copied in mail
        global G_MESSAGE_CC_CONTACTS
        G_MESSAGE_CC_CONTACTS = CONFIG['ApplicationParams\
                                    ']['to_string']

        """
        Location of lag report output file for Transmit Layer having only
        total lag count called Transmit Report
        """
        global G_LAG_REPORT_TRANSMIT_OUTPUT_FILE
        G_LAG_REPORT_TRANSMIT_OUTPUT_FILE = CONFIG['ApplicationParams\
                                        ']['lag_report_transmit_output_file']

        """
        Location of lag report output file for Transmit Layer having metrics
        consumer and partition wise called Transmit Main Report
        """
        global G_LAG_REPORT_MAIN_TRANSMIT_OUTPUT_FILE
        G_LAG_REPORT_MAIN_TRANSMIT_OUTPUT_FILE = CONFIG['ApplicationParams\
                                    ']['lag_report_main_transmit_output_file']

        """
        Location of lag report for combined Transmit Report, i.e.,
        having all 13 message types report
        """
        global G_LAG_REPORT_COMBINED_TRANSMIT_OUTPUT_FILE
        G_LAG_REPORT_COMBINED_TRANSMIT_OUTPUT_FILE = CONFIG['ApplicationParam\
                            s']['lag_report_combined_transmit_output_file']

        """
        Location of lag report for combined Transmit Main Report, i.e.,
        having all 13 message types report
        """
        global G_LAG_REPORT_COMBINED_MAIN_TRANSMIT_OUTPUT_FILE
        G_LAG_REPORT_COMBINED_MAIN_TRANSMIT_OUTPUT_FILE = CONFIG['Application\
                    Params']['lag_report_combined_main_transmit_output_file']

        # Topic Names for Transmit Layer
        global G_TOPIC_NAMES_TRANSMIT
        G_TOPIC_NAMES_TRANSMIT = CONFIG['ApplicationParams\
                                        ']['topic_names_transmit']

        # Consumer group names for Transmit Layer
        global G_CONSUMER_GROUP_NAMES_TRANSMIT
        G_CONSUMER_GROUP_NAMES_TRANSMIT = CONFIG['ApplicationParams\
                                                ']['consumer_groups_transmit']

        """
        Location of lag report output file for Ingest Layer having metrics
        consumer and partition wise called Ingest Main Report
        """
        global G_LAG_REPORT_MAIN_INGEST_OUTPUT_FILE
        G_LAG_REPORT_MAIN_INGEST_OUTPUT_FILE = CONFIG['ApplicationParams\
        ']['lag_report_main_ingest_output_file']

        """
        Location of lag report for combined Ingest Main Report, i.e.,
        having all 13 message types report
        """
        global G_LAG_REPORT_COMBINED_MAIN_INGEST_OUTPUT_FILE
        G_LAG_REPORT_COMBINED_MAIN_INGEST_OUTPUT_FILE = CONFIG['Application\
                    Params']['lag_report_combined_main_ingest_output_file']

        # Topic Names for Ingest Layer
        global G_TOPIC_NAMES_INGEST
        G_TOPIC_NAMES_INGEST = CONFIG['ApplicationParams\
                                    ']['topic_names_ingest']

        # Consumer group names for Ingest Layer
        global G_CONSUMER_GROUP_NAMES_INGEST
        G_CONSUMER_GROUP_NAMES_INGEST = CONFIG['ApplicationParams\
                                            ']['consumer_groups_ingest']

        """
        Location of lag report output file for Persist Retry Layer
        having metrics consumer and partition wise called
        Persist Retry Main Report
        """
        global G_LAG_REPORT_MAIN_PERSIST_RETRY_OUTPUT_FILE
        G_LAG_REPORT_MAIN_PERSIST_RETRY_OUTPUT_FILE = CONFIG['ApplicationParam\
                            s']['lag_report_main_persist_retry_output_file']

        """
        Location of lag report for combined Persist Retry Main Report,
        i.e., having all 13 message types report
        """
        global G_LAG_REPORT_COMBINED_MAIN_PERSIST_RETRY_OUTPUT_FILE
        G_LAG_REPORT_COMBINED_MAIN_PERSIST_RETRY_OUTPUT_FILE = CONFIG['\
                            ApplicationParams']['lag_report_combined_main\
                            _persist_retry_output_file']

        # Topic Names for Persist Retry Layer
        global G_TOPIC_NAMES_PERSIST_RETRY
        G_TOPIC_NAMES_PERSIST_RETRY = CONFIG['ApplicationParams\
                                            ']['topic_names_persist_retry']

        # Consumer group names for Persist Retry Layer
        global G_CONSUMER_GROUP_NAMES_PERSIST_RETRY
        G_CONSUMER_GROUP_NAMES_PERSIST_RETRY = CONFIG['ApplicationParams\
                                        ']['consumer_groups_persist_retry']

        """
        Location of lag report output file for Pre-Transmit Retry Layer
        having metrics consumer and partition wise called
        Pre-Transmit Retry Main Report
        """
        global G_LAG_REPORT_MAIN_PRETRANSMIT_RETRY_OUTPUT_FILE
        G_LAG_REPORT_MAIN_PRETRANSMIT_RETRY_OUTPUT_FILE = CONFIG['Application\
                    Params']['lag_report_main_pre_transmit_retry_output_file']

        """
        Location of lag report for combined Pre-Transmit Retry Main Report,
        i.e., having all 13 message types report
        """
        global G_LAG_REPORT_COMBINED_MAIN_PRETRANSMIT_RETRY_OUTPUT_FILE
        G_LAG_REPORT_COMBINED_MAIN_PRETRANSMIT_RETRY_OUTPUT_FILE = CONFIG['\
        ApplicationParams']['lag_report_combined_main_pre_transmit_retry_\
        output_file']

        # Topic Names for Pre-Transmit Retry Layer
        global G_TOPIC_NAMES_PRETRANSMIT_RETRY
        G_TOPIC_NAMES_PRETRANSMIT_RETRY = CONFIG['ApplicationParams\
                                        ']['topic_names_pre_transmit_retry']

        # Consumer group names for Pre-Transmit Retry Layer
        global G_CONSUMER_GROUP_NAMES_PRETRANSMIT_RETRY
        G_CONSUMER_GROUP_NAMES_PRETRANSMIT_RETRY = CONFIG['ApplicationParams\
                                    ']['consumer_groups_pre_transmit_retry']

        """
        Location of lag report output file for Transmit Retry Layer having
        metrics consumer and partition wise called Transmit Retry Main Report
        """
        global G_LAG_REPORT_MAIN_TRANSMIT_RETRY_OUTPUT_FILE
        G_LAG_REPORT_MAIN_TRANSMIT_RETRY_OUTPUT_FILE = CONFIG['Application\
                        Params']['lag_report_main_transmit_retry_output_file']

        """
        Location of lag report for combined Transmit Retry Main Report, i.e.,
        having all 13 message types report
        """
        global G_LAG_REPORT_COMBINED_MAIN_TRANSMIT_RETRY_OUTPUT_FILE
        G_LAG_REPORT_COMBINED_MAIN_TRANSMIT_RETRY_OUTPUT_FILE = CONFIG['\
        ApplicationParams']['lag_report_combined_main_transmit_retry_\
        output_file']

        # Topic Names for Transmit Retry Layer
        global G_TOPIC_NAMES_TRANSMIT_RETRY
        G_TOPIC_NAMES_TRANSMIT_RETRY = CONFIG['ApplicationParams']['\
                                            topic_names_transmit_retry']

        # Consumer group names for Transmit Retry Layer
        global G_CONSUMER_GROUP_NAMES_TRANSMIT_RETRY
        G_CONSUMER_GROUP_NAMES_TRANSMIT_RETRY = CONFIG['ApplicationParams\
                                        ']['consumer_groups_transmit_retry']

        """
        Location of lag report output file for MetricsDB (PostgreSQL Events)
        having metrics consumer and partition wise called MetricsDB Report
        """
        global G_LAG_REPORT_MAIN_METRICSDB_OUTPUT_FILE
        G_LAG_REPORT_MAIN_METRICSDB_OUTPUT_FILE = CONFIG['ApplicationParams\
                                ']['lag_report_main_metricsdb_output_file']

        """
        Location of lag report for combined MetricsDB (PostgreSQL Events)
        Report, i.e., having all 13 message types report
        """
        global G_LAG_REPORT_COMBINED_MAIN_METRICSDB_OUTPUT_FILE
        G_LAG_REPORT_COMBINED_MAIN_METRICSDB_OUTPUT_FILE = CONFIG['\
        ApplicationParams']['lag_report_combined_main_metricsdb_output_file']

        # Topic Names for MetricsDB (PostgreSQL Events)
        global G_TOPIC_NAMES_METRICSDB
        G_TOPIC_NAMES_METRICSDB = CONFIG['ApplicationParams']['\
                                        topic_names_metricsdb']

        # Consumer group names for MetricsDB (PostgreSQL Events)
        global G_CONSUMER_GROUP_NAMES_METRICSDB
        G_CONSUMER_GROUP_NAMES_METRICSDB = CONFIG['ApplicationParams']['\
                                                consumer_groups_metricsdb']

        # Slack Bot token to connect to Slack
        global G_SLACK_CHANNEL_TOKEN
        G_SLACK_CHANNEL_TOKEN = CONFIG['ApplicationParams']['slack_token']

        # Reports send to this Slack Channel Name
        global G_SLACK_CHANNEL_NAME
        G_SLACK_CHANNEL_NAME = CONFIG['ApplicationParams']['\
                                    slack_channel_name']

    except Exception as e:
        raise Exception('Exception encountered in set_env() while setting up \
        application configuration parameters.')


# This function is used for reading the names and emails.
def get_contacts(G_CONTACT_INPUT_FILE):
    """
    Return two lists names, emails containing names and email addresses
    read from a file specified by filename.
    """

    names = []
    emails = []
    with open(G_CONTACT_INPUT_FILE, mode='r') as contacts_file:
        for a_contact in contacts_file:
            names.append(a_contact.split()[0])
            emails.append(a_contact.split()[1])
    return names, emails


# This function is used for saving the template from user provided content.
def read_template(G_MESSAGE_INPUT_FILE):
    """
    Returns a Template object comprising the contents of the
    file specified by filename.
    """

    with open(G_MESSAGE_INPUT_FILE, 'r') as template_file:
        template_file_content = template_file.read()
    return Template(template_file_content)


# This function is used for saving the lag statistics for Kafka Broker.
def process_topic(topic_name, consumer_group):
    # To record Amsterdam Datacenter Statistics based on topic name
    f_ams = open("\
        /root/new_reports/output/intermediate_queue_reports/kafka_report_ams_\
        " + topic_name + "_.txt", "w+")
    # To record Frankfurt Datacenter Statistics based on topic name
    f_fra = open("\
        /root/new_reports/output/intermediate_queue_reports/kafka_report_fra_\
        " + topic_name + "_.txt", "w+")
    """
    Command to list the kafka brokers and record the live brokers into list.
    This is required to provide the high availability. Code should not fail\
    in case of kafka broker down.
    Once the available broker will be recorded then to record the lag of the\
    topic.
    """
    # Below lines of code for Frankfurt Datacenter
    kafka_command = 'cd /opt/kafka_2.11-0.10.2.1/kafka_2.11-0.10.2.1/bin;\
    ./zookeeper-shell.sh kafka03:2181 <<< "ls /brokers/ids"'
    commandOutput = subprocess.Popen(
        ["ssh", "%s" % HOST, kafka_command],
        shell=False, stdout=subprocess.PIPE, stderr=None)
    output = commandOutput.communicate()
    LinesList = output[0].split("\n")
    kafkaBrokerListFRA = LinesList[-2].split(", ")
    kafkaBrokers = []
    if kafkaBrokerListFRA:
        for i in kafkaBrokerListFRA:
            newKafka = str(i.replace("[", "")).replace("]", "")
            kafkaBrokers.append(newKafka)
            if str(1) in kafkaBrokers:
                kafka_command = "cd /opt/kafka_2.11-0.10.2.1/kafka_2.11-0.10.2.1/bin\
                ; ./kafka-consumer-offset-checker.sh --zookeeper=kafka03:\
                2181 --topic=" + topic_name + " --group=" + consumer_group
                subprocess.Popen(
                    ["ssh", "%s" % HOST, kafka_command], shell=False, stdout=f_fra)
            elif str(2) in kafkaBrokers:
                kafka_command = "cd /opt/kafka_2.11-0.10.2.1/kafka_2.11-0.10.2.1/bin\
                ; ./kafka-consumer-offset-checker.sh --zookeeper=kafka04:\
                2181 --topic=" + topic_name + " --group=" + consumer_group
                subprocess.Popen(
                    ["ssh", "%s" % HOST, kafka_command], shell=False, stdout=f_fra)
            elif str(3) in kafkaBrokers:
                kafka_command = "cd /opt/kafka_2.11-0.10.2.1/kafka_2.11-0.10.2.1/bin\
                ; ./kafka-consumer-offset-checker.sh --zookeeper=kafka05:\
                2181 --topic=" + topic_name + " --group=" + consumer_group
                subprocess.Popen(
                    ["ssh", "%s" % HOST, kafka_command], shell=False, stdout=f_fra)
    # Below lines of code for Amsterdam Datacenter
    kafka_command = 'cd /opt/kafka_2.11-0.10.2.1/kafka_2.11-0.10.2.1/bin\
    ; ./zookeeper-shell.sh kafka01:2181 <<< "ls /brokers/ids"'
    commandOutput = subprocess.Popen(
        kafka_command, shell=True, stdout=subprocess.PIPE, stderr=None)
    output = commandOutput.communicate()
    LinesList = output[0].split("\n")
    kafkaBrokerListAMS = LinesList[-2].split(", ")
    kafkaBrokers = []
    if kafkaBrokerListAMS:
        for i in kafkaBrokerListAMS:
            newKafka = str(i.replace("[", "")).replace("]", "")
            kafkaBrokers.append(newKafka)
            if str(1) in kafkaBrokers:
                kafka_command = "cd /opt/kafka_2.11-0.10.2.1/kafka_2.11-0.10.2.1/bin\
                ; ./kafka-consumer-offset-checker.sh --zookeeper=kafka01:\
                2181 --topic=" + topic_name + " --group=" + consumer_group
                subprocess.call(kafka_command, shell=True, stdout=f_ams)
            elif str(2) in kafkaBrokers:
                kafka_command = "cd /opt/kafka_2.11-0.10.2.1/kafka_2.11-0.10.2.1/bin\
                ; ./kafka-consumer-offset-checker.sh --zookeeper=kafka02:\
                2181 --topic=" + topic_name + " --group=" + consumer_group
                subprocess.call(kafka_command, shell=True, stdout=f_ams)
            elif str(3) in kafkaBrokers:
                kafka_command = "cd /opt/kafka_2.11-0.10.2.1/kafka_2.11-0.10.2.1/bin\
                ; ./kafka-consumer-offset-checker.sh --zookeeper=kafka03:\
                2181 --topic=" + topic_name + " --group=" + consumer_group
                subprocess.call(kafka_command, shell=True, stdout=f_ams)

    """
    Put null check in code to avoid failure.
    Provide the total lagsize, lag metrics, logsize and logsize metrics to\
    the excel report.
    """
    if kafkaBrokerListFRA or kafkaBrokerListAMS:
        fileList_fra = [line.rstrip('\n') for line in open('\
        /root/new_reports/output/intermediate_queue_reports/kafka_report_fra_\
        ' + topic_name + '_.txt')]
        fileList_ams = [line.rstrip('\n') for line in open('\
        /root/new_reports/output/intermediate_queue_reports/kafka_report_ams_\
        ' + topic_name + '_.txt')]
        currentTotalLagFRA = 0
        currentTotalLagFRAList = []
        currentTotalLagAMS = 0
        currentTotalLagAMSList = []
        currentTotalLogSizeFRA = 0
        currentTotalLogSizeFRAList = []
        currentTotalLogSizeAMS = 0
        currentTotalLogSizeAMSList = []
        i = 1
        for lines in fileList_fra:
            columnList = lines.split("\\n")
            for col in columnList:
                realColList = col.split(" ")
                colListWithoutSpace = []
                for rc in realColList:
                    if rc == " " or rc == "":
                        pass
                    else:
                        colListWithoutSpace.append(rc)
                if i != 1 and i >= 2:
                    currentTotalLagFRA += int(colListWithoutSpace[5])
                    currentTotalLagFRAList.append(int(colListWithoutSpace[5]))
                    currentTotalLogSizeFRA += int(colListWithoutSpace[4])
                    currentTotalLogSizeFRAList.append(int(
                        colListWithoutSpace[4]))
                i = i + 1
        i = 1
        for lines in fileList_ams:
            columnList = lines.split("\\n")
            for col in columnList:
                realColList = col.split(" ")
                colListWithoutSpace = []
                for rc in realColList:
                    if rc == " " or rc == "":
                        pass
                    else:
                        colListWithoutSpace.append(rc)
                if i != 1 and i >= 2:
                    currentTotalLagAMS += int(colListWithoutSpace[5])
                    currentTotalLagAMSList.append(int(colListWithoutSpace[5]))
                    currentTotalLogSizeAMS += int(colListWithoutSpace[4])
                    currentTotalLogSizeAMSList.append(int(
                        colListWithoutSpace[4]))
                i = i + 1

        return \
            currentTotalLagFRA, currentTotalLagAMS, \
            currentTotalLagFRAList, currentTotalLagAMSList, \
            currentTotalLogSizeFRA, currentTotalLogSizeAMS, \
            currentTotalLogSizeFRAList, currentTotalLogSizeAMSList
    else:
        currentTotalLagFRA = 0
        currentTotalLagAMS = 0
        currentTotalLagFRAList = []
        currentTotalLagAMSList = []
        currentTotalLogSizeFRA = 0
        currentTotalLogSizeAMS = 0
        currentTotalLogSizeFRAList = []
        currentTotalLogSizeAMSList = []
        return \
            currentTotalLagFRA, currentTotalLagAMS, \
            currentTotalLagFRAList, currentTotalLagAMSList, \
            currentTotalLogSizeFRA, currentTotalLogSizeAMS, \
            currentTotalLogSizeFRAList, currentTotalLogSizeAMSList


# Record the date and time for UK Time Zone for client feasibility.
def set_up_date_time():
    # set up date and time
    date_time = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
    time_uktime_format = date_time.strftime("%H:%M")
    date_uktime_format = date_time.strftime("%d %B")
    return time_uktime_format, date_uktime_format


# This function is used for consolidating Kafka metrics in excel sheet.
def collating_results(
        G_TOPIC_NAMES, G_CONSUMER_GROUP_NAMES,
        G_LAG_REPORT_MAIN_OUTPUT_FILE,
        G_LAG_REPORT_COMBINED_MAIN_OUTPUT_FILE,
        G_LAG_REPORT_OUTPUT_FILE=None,
        G_LAG_REPORT_COMBINED_OUTPUT_FILE=None):
    topic_name_list = G_TOPIC_NAMES.split(",")
    consumer_group_names = G_CONSUMER_GROUP_NAMES.split(",")
    topic_dict = zip(topic_name_list, consumer_group_names)
    # Looping all topic names and consumer group names
    for topic_name, consumer_group in topic_dict:
        word = topic_name
        message_type_list = word.split('-')[0:3]
        s = [str(i) for i in message_type_list]
        message_type = str("-".join(s))

        currentTotalLagFRA, currentTotalLagAMS, \
        currentTotalLagFRAList, currentTotalLagAMSList, \
        currentTotalLogSizeFRA, currentTotalLogSizeAMS, \
        currentTotalLogSizeFRAList, \
        currentTotalLogSizeAMSList = process_topic(
            topic_name,
            consumer_group)

        # set up date and time
        time_uktime_format = ""
        date_uktime_format = ""
        try:
            time_uktime_format, date_uktime_format = set_up_date_time()
        except Exception as e:
            print("Exception in date: ", e)

        # Updating kafka metrics in excel sheet
        ExcelReportGenerate.excelSheetUpdation(
            currentTotalLagFRA,
            currentTotalLagAMS,
            currentTotalLagFRAList,
            currentTotalLagAMSList,
            date_uktime_format,
            time_uktime_format,
            currentTotalLogSizeFRA,
            currentTotalLogSizeAMS,
            currentTotalLogSizeFRAList,
            currentTotalLogSizeAMSList,
            message_type,
            G_LAG_REPORT_MAIN_OUTPUT_FILE,
            G_LAG_REPORT_OUTPUT_FILE)

    # Combining all thirteen message-types
    try:
        CombinedExcelReport.message_types_together(
            G_LAG_REPORT_MAIN_OUTPUT_FILE,
            G_LAG_REPORT_COMBINED_MAIN_OUTPUT_FILE,
            G_LAG_REPORT_OUTPUT_FILE,
            G_LAG_REPORT_COMBINED_OUTPUT_FILE)
    except Exception as e:
        print("Exception: ", e)


# This function is used for attaching the excel sheet.
def send_attachments(attachment_file, file_name, msg):
    attachment = open(attachment_file, 'rb')
    xlsx = MIMEBase('application',
                    'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    xlsx.set_payload(attachment.read())

    encoders.encode_base64(xlsx)
    xlsx.add_header('Content-Disposition', 'attachment', filename=file_name)
    msg.attach(xlsx)
    return msg


# This function is calling the above methods and sending reports via email.
def generate_reports():
    names, emails = get_contacts(G_CONTACT_INPUT_FILE)
    message_template = read_template(G_MESSAGE_INPUT_FILE)

    try:
        # Calling for Persist Layer
        collating_results(
            G_TOPIC_NAMES_INGEST,
            G_CONSUMER_GROUP_NAMES_INGEST,
            G_LAG_REPORT_MAIN_INGEST_OUTPUT_FILE,
            G_LAG_REPORT_COMBINED_MAIN_INGEST_OUTPUT_FILE)

        # Calling for Transmit Layer
        collating_results(
            G_TOPIC_NAMES_TRANSMIT,
            G_CONSUMER_GROUP_NAMES_TRANSMIT,
            G_LAG_REPORT_MAIN_TRANSMIT_OUTPUT_FILE,
            G_LAG_REPORT_COMBINED_MAIN_TRANSMIT_OUTPUT_FILE,
            G_LAG_REPORT_TRANSMIT_OUTPUT_FILE,
            G_LAG_REPORT_COMBINED_TRANSMIT_OUTPUT_FILE)

        # Calling for Persist_Retry Layer
        collating_results(
            G_TOPIC_NAMES_PERSIST_RETRY,
            G_CONSUMER_GROUP_NAMES_PERSIST_RETRY,
            G_LAG_REPORT_MAIN_PERSIST_RETRY_OUTPUT_FILE,
            G_LAG_REPORT_COMBINED_MAIN_PERSIST_RETRY_OUTPUT_FILE)

        # Calling for Pre_Transmit_Retry Layer
        collating_results(
            G_TOPIC_NAMES_PRETRANSMIT_RETRY,
            G_CONSUMER_GROUP_NAMES_PRETRANSMIT_RETRY,
            G_LAG_REPORT_MAIN_PRETRANSMIT_RETRY_OUTPUT_FILE,
            G_LAG_REPORT_COMBINED_MAIN_PRETRANSMIT_RETRY_OUTPUT_FILE)

        # Calling for Transmit_Retry Layer
        collating_results(
            G_TOPIC_NAMES_TRANSMIT_RETRY,
            G_CONSUMER_GROUP_NAMES_TRANSMIT_RETRY,
            G_LAG_REPORT_MAIN_TRANSMIT_RETRY_OUTPUT_FILE,
            G_LAG_REPORT_COMBINED_MAIN_TRANSMIT_RETRY_OUTPUT_FILE)

        # Calling for MetricsDB Layer
        collating_results(
            G_TOPIC_NAMES_METRICSDB,
            G_CONSUMER_GROUP_NAMES_METRICSDB,
            G_LAG_REPORT_MAIN_METRICSDB_OUTPUT_FILE,
            G_LAG_REPORT_COMBINED_MAIN_METRICSDB_OUTPUT_FILE)

        # For each contact, send the email:
        for name, email in zip(names, emails):
            msg = MIMEMultipart()  # create a message

            time_uktime_format, date_uktime_format = set_up_date_time()

            # add something to the message template
            message = message_template.substitute()
            # Prints out the message body for our sake
            print(message)

            # setup the parameters of the message
            msg['From'] = SENDEREMAILADD
            msg['To'] = str(G_MESSAGE_CC_CONTACTS)
            msg['Subject'] = "Consolidated Hourly Report : \
                        " + time_uktime_format + " UK Time, \
                        " + date_uktime_format + " Live Queues For All \
                        Message Type"

            # add in the message body
            msg.attach(MIMEText(message, 'plain'))

            # Attaching report for Persist Layer
            msg = send_attachments(
                G_LAG_REPORT_COMBINED_MAIN_INGEST_OUTPUT_FILE,
                'Message_Persist_Main_Queue_Lag_Report_IBM.xlsx',
                msg)
            # Attaching report for Transmit Layer
            msg = send_attachments(
                G_LAG_REPORT_COMBINED_MAIN_TRANSMIT_OUTPUT_FILE,
                'Message_Transmit_Main_Queue_Lag_Report_IBM.xlsx',
                msg)
            # Attaching report for Persist_Retry Layer
            msg = send_attachments(
                G_LAG_REPORT_COMBINED_MAIN_PERSIST_RETRY_OUTPUT_FILE,
                'Message_Persist_Retry_Lag_Report_IBM.xlsx',
                msg)
            # Attaching report for Pre_Transmit_Retry Layer
            msg = send_attachments(
                G_LAG_REPORT_COMBINED_MAIN_PRETRANSMIT_RETRY_OUTPUT_FILE,
                'Message_Pre_Transmit_Retry_Queue_Lag_Report_IBM.xlsx',
                msg)
            # Attaching report for Transmit_Retry Layer
            msg = send_attachments(
                G_LAG_REPORT_COMBINED_MAIN_TRANSMIT_RETRY_OUTPUT_FILE,
                'Message_Transmit_Retry_Queue_Lag_Report_IBM.xlsx',
                msg)
            # Attaching report for MetricsDB Layer
            msg = send_attachments(
                G_LAG_REPORT_COMBINED_MAIN_METRICSDB_OUTPUT_FILE,
                'Message_MetricsDB_Queue_Lag_Report_IBM.xlsx',
                msg)

            # Send Mail via SMTP
            try:
                smtpObj = smtplib.SMTP('localhost')
                smtpObj.sendmail(SENDEREMAILADD, str(email), str(msg))
                print("Successfully sent email")
            except SMTPException:
                print("Error: unable to send email")
                del msg

    except Exception as e:
        print(e)


# Send reports in Slack Channel via Slack Bot.
def post_in_slack():
    try:
        with open(G_LAG_REPORT_COMBINED_MAIN_INGEST_OUTPUT_FILE, 'rb') as f:
            param = {
                'token': G_SLACK_CHANNEL_TOKEN,
                'channels': G_SLACK_CHANNEL_NAME,
                'title': 'Message_Persist_Main_Queue_Lag_Report'
            }
            r = requests.post(
                "https://slack.com/api/files.upload",
                params=param,
                files={'file': f}
            )
            print("Response received from Slack: ", r.text)
        with open(G_LAG_REPORT_COMBINED_MAIN_TRANSMIT_OUTPUT_FILE, 'rb') as f:
            param = {
                'token': G_SLACK_CHANNEL_TOKEN,
                'channels': G_SLACK_CHANNEL_NAME,
                'title': 'Message_Transmit_Main_Queue_Lag_Report'
            }
            r = requests.post(
                "https://slack.com/api/files.upload",
                params=param,
                files={'file': f}
            )
            print("Response received from Slack: ", r.text)
        with open(
                G_LAG_REPORT_COMBINED_MAIN_PERSIST_RETRY_OUTPUT_FILE,
                'rb') as f:
            param = {
                'token': G_SLACK_CHANNEL_TOKEN,
                'channels': G_SLACK_CHANNEL_NAME,
                'title': 'Message_Persist_Retry_Lag_Report'
            }
            r = requests.post(
                "https://slack.com/api/files.upload",
                params=param,
                files={'file': f}
            )
            print("Response received from Slack: ", r.text)
        with open(
                G_LAG_REPORT_COMBINED_MAIN_PRETRANSMIT_RETRY_OUTPUT_FILE,
                'rb') as f:
            param = {
                'token': G_SLACK_CHANNEL_TOKEN,
                'channels': G_SLACK_CHANNEL_NAME,
                'title': 'Message_Pre_Transmit_Retry_Queue_Lag_Report'
            }
            r = requests.post(
                "https://slack.com/api/files.upload",
                params=param,
                files={'file': f}
            )
            print("Response received from Slack: ", r.text)
        with open(
                G_LAG_REPORT_COMBINED_MAIN_TRANSMIT_RETRY_OUTPUT_FILE,
                'rb') as f:
            param = {
                'token': G_SLACK_CHANNEL_TOKEN,
                'channels': G_SLACK_CHANNEL_NAME,
                'title': 'Message_Transmit_Retry_Queue_Lag_Report'
            }
            r = requests.post(
                "https://slack.com/api/files.upload",
                params=param,
                files={'file': f}
            )
            print("Response received from Slack: ", r.text)
        with open(G_LAG_REPORT_COMBINED_MAIN_METRICSDB_OUTPUT_FILE, 'rb') as f:
            param = {
                'token': G_SLACK_CHANNEL_TOKEN,
                'channels': G_SLACK_CHANNEL_NAME,
                'title': 'Message_MetricsDB_Queue_Lag_Report'
            }
            r = requests.post(
                "https://slack.com/api/files.upload",
                params=param,
                files={'file': f}
            )
            print("Response received from Slack: ", r.text)
    except Exception as e:
        print("Main Exception is: ", e)


# This is the main function to call the required functions
def main():
    """
    Usage: python lag_report.py lag_report.ini
    :return:
    """

    try:
        # Extract command line parameters.
        P_APP_CONFIG_FILE = extract_command_params(sys.argv)

        # Set environment.
        set_env(P_APP_CONFIG_FILE)

        # Process lag and logSize metrics and send reports via email
        generate_reports()

        # Post in Slack
        post_in_slack()

    except Exception as e:
        traceback.print_exc()
        raise Exception('Exception message')


# Conditionally invoke the main() function to executed from command line
if __name__ == "__main__":
    main()
