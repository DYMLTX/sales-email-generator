"""Setup script for MAX.Live Email Automation System."""

from setuptools import setup, find_packages

setup(
    name="maxlive-email-automation",
    version="0.1.0",
    description="Automated sales email system for MAX.Live show sponsorships",
    author="MAX.Live",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        line.strip()
        for line in open("requirements.txt")
        if line.strip() and not line.startswith("#")
    ],
    entry_points={
        "console_scripts": [
            "maxlive-db=src.database.explore_db:cli",
        ],
    },
)