from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage

import pandas as pd
import matplotlib.pyplot as plt

from dataops import data_quality_report_numerical, data_quality_report_categorical
from typing import Callable

# fill in your own email address and password
smtp_server = "smtp.gmail.com"
smtp_port = 587
smtp_username = "your_email@gmail.com"
smtp_password = "app_password"
sender_email = "your_email@gmail.com"
receiver_email = "your_email@gmail.com"


def text_email() -> MIMEMultipart:
    subject = "Data Profiling Report"
    body = "Please find attached the Data Profiling report(s)."

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain"))

    return msg


def add_report(
    msg: MIMEMultipart, data_path: str, fn: Callable, reportname: str
) -> MIMEMultipart:
    data = pd.read_csv(data_path)
    report = fn(data)
    csv_report = report.to_csv(index=True)
    attachment = MIMEApplication(csv_report)
    attachment.add_header(
        "Content-Disposition", "attachment", filename=f"{reportname}.csv"
    )
    msg.attach(attachment)
    return msg


def send_email(msg: MIMEMultipart) -> None:
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        try:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
            print("Email successfully sent")
        except:
            print("An exception occurred when sending the data profiling email...")
    return


def send_data_profiling_email(data_path: str) -> None:
    text = text_email()
    report1 = add_report(
        text, data_path, data_quality_report_numerical, "data_quality_report_1"
    )
    report2 = add_report(
        report1, data_path, data_quality_report_categorical, "data_quality_report_2"
    )
    send_email(report2)
    return