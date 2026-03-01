from setuptools import setup, find_packages


def readme():
  with open("README.md", "r") as file:
    return file.read()


setup(
  name="melony",
  version="1.3.1",
  author="wimble3",
  author_email="wimble@internet.ru",
  description='Multisync task manager for python',
  long_description=readme(),
  long_description_content_type="text/markdown",
  url="https://melony-python.com/",
  packages=find_packages(),
  install_requires=[
    "asyncio>=4.0.0",
    "classes>=0.4.1",
    "croniter>=3.0.0",
    "pytest>=8.4.1",
    "pytest-asyncio>=1.1.0",
    "pytest-cov>=6.0.0",
    "redis>=6.4.0",
    "wemake-python-styleguide>=1.3.0",
  ],
  classifiers=[
    "Programming Language :: Python :: 3.12",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent"
  ],
  keywords='task manager python melony',
  project_urls={
    "Documentation": "https://melony-python.com/docs"
  },
  python_requires='>=3.12'
)
