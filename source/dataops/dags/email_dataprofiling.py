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

from dataops import (
    data_quality_report_numerical,
    data_quality_report_categorical,
    categorical_charts,
    numerical_charts,
)
from typing import Callable

# fill in your own email address
smtp_server = "smtp.gmail.com"
smtp_port = 587
smtp_username = "bt4301group8@gmail.com"
smtp_password = "kbkpubaotefnlxpi"
sender_email = "bt4301group8@gmail.com"
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


def add_image(
    msg: MIMEMultipart, data_path: str, fn: Callable, chart_name: str
) -> MIMEMultipart:
    data = pd.read_csv(data_path)
    image_path = fn(data, chart_name)
    with open(image_path, "rb") as f:
        img = MIMEImage(f.read())
        img.add_header("Content-Disposition", "attachment", filename=image_path)
        msg.attach(img)
        return msg


def send_data_profiling_email(data_path: str) -> None:
    text = text_email()
    report1 = add_report(
        text, data_path, data_quality_report_numerical, "data_quality_report_1"
    )
    report2 = add_report(
        report1, data_path, data_quality_report_categorical, "data_quality_report_2"
    )
    chart1 = add_image(report2, data_path, numerical_charts, data_path[:-4])
    chart2 = add_image(chart1, data_path, categorical_charts, data_path[:-4])
    send_email(chart2)
    return
