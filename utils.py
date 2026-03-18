#!/usr/bin/env python3
"""Utility functions for data tooling ETL processes"""

import configparser
import psycopg2
import logging
from pyspark.sql import SparkSession


def read_config(file_path="etl.config"):
    """Read configuration from file

    Args:
        file_path (str): Path to config file

    Returns:
        configparser.RawConfigParser: Configuration object
    """
    config_obj = configparser.RawConfigParser()
    config_obj.read(file_path)
    return config_obj


def get_db_connection(config):
    """Create PostgreSQL database connection

    Args:
        config (configparser.RawConfigParser): Configuration object

    Returns:
        tuple: (connection, cursor) objects
    """
    try:
        conn = psycopg2.connect(
            database=config.get("reportdb", "dbname"),
            host=config.get("reportdb", "host"),
            user=config.get("reportdb", "dbuser"),
            password=config.get("reportdb", "dbuserpass"),
            port=config.get("reportdb", "port"),
        )
        cursor = conn.cursor()
        return conn, cursor
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        raise


def get_spark_session(app_name, config):
    """Create Apache Spark session

    Args:
        app_name (str): Application name for Spark
        config (configparser.RawConfigParser): Configuration object

    Returns:
        SparkSession: Configured Spark session
    """
    spark_master = config.get("spark", "master")
    spark_master_port = config.get("spark", "masterport")
    aws_access_key = config.get("aws", "access.key")
    aws_secret_key = config.get("aws", "secret.key")

    return (
        SparkSession.builder.appName(app_name)
        .master(f"spark://{spark_master}:{spark_master_port}")
        .config("spark.executor.memory", "14g")
        .config("spark.executor.cores", "4")
        .config("spark.task.cpus", "4")
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .config("spark.debug.maxToStringFields", "1000")
        .config(
            "spark.jars",
            "/var/tmp/sparkjars/postgresql-42.6.0.jar,"
            "/var/tmp/sparkjars/aws-java-sdk-bundle-1.12.262.jar,"
            "/var/tmp/sparkjars/hadoop-aws-3.3.4.jar",
        )
        .getOrCreate()
    )


def setup_logging(level=logging.INFO):
    """Setup logging configuration

    Args:
        level: Logging level (default: INFO)
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler()],
    )
