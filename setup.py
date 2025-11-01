"""
Setup configuration for crypto-etl-pipeline
"""
from setuptools import setup, find_packages

setup(
    name="crypto-etl-pipeline",
    version="0.1.0",
    description="End-to-end ETL pipeline for cryptocurrency data",
    author="Your Name",
    packages=find_packages(exclude=["tests*", "scripts*"]),
    python_requires=">=3.9",
    install_requires=[
        "requests>=2.31.0",
        "python-dotenv>=1.0.0",
        "pandas>=2.1.4",
        "numpy>=1.26.2",
        "sqlalchemy>=2.0.23",
        "psycopg2-binary>=2.9.9",
        "pydantic>=2.5.3",
        "pyyaml>=6.0.1",
        "loguru>=0.7.2",
        "schedule>=1.2.0",
        "plotly>=5.18.0",
        "matplotlib>=3.8.2",
        "seaborn>=0.13.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "black>=23.12.1",
            "flake8>=7.0.0",
            "isort>=5.13.2",
        ]
    },
)