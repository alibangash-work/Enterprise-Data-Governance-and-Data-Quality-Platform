"""
Setup script for Data Governance Platform
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="data-governance-platform",
    version="1.0.0",
    author="Data Governance Team",
    author_email="team@data-governance-platform.io",
    description="Enterprise Data Governance Platform with Quality, Lineage, and Observability",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/data-governance-platform",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.11",
    install_requires=[
        "pyspark==3.5.0",
        "delta-spark==3.0.0",
        "fastapi==0.104.1",
        "uvicorn==0.24.0",
        "pydantic==2.4.2",
        "pandas==2.0.3",
        "numpy==1.24.3",
        "great-expectations==0.18.4",
        "prometheus-client==0.18.0",
        "apache-airflow==2.7.0",
        "sqlalchemy==2.0.22",
        "psycopg2-binary==2.9.9",
        "python-dotenv==1.0.0",
        "pyyaml==6.0.1",
        "requests==2.31.0",
    ],
    extras_require={
        "dev": [
            "pytest==7.4.3",
            "pytest-cov==4.1.0",
            "black==23.11.0",
            "isort==5.13.2",
            "flake8==6.1.0",
            "mypy==1.7.1",
        ],
        "aws": [
            "boto3==1.28.0",
            "s3fs==2023.10.0",
        ],
        "azure": [
            "azure-storage-blob==12.18.0",
            "adlfs==2023.10.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "data-gov-api=api.main:app",
        ],
    },
)
