from setuptools import setup, find_packages

setup(
    name = "moira",
    version = "0.1.0",
    author = "SKB Kontur",
    author_email = "devops@skbkontur.ru",
    description = "Moira checker and api modules",
    license = "GPLv3",
    keywords = "moira graphite alert monitoring",
    url = "http://github.com/moira-alert/worker",
    packages=find_packages(exclude=['tests']),
    long_description='Please, visit moira.readthedocs.org for more information',
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    ],
)