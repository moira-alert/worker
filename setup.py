from setuptools import setup, find_packages

with open('requirements.txt') as f:
    required = f.read().splitlines()

with open('version.txt') as f:
    version = f.read().strip()

setup(
    name = "moira_worker",
    author = "SKB Kontur",
    version = version,
    author_email = "devops@skbkontur.ru",
    description = "Moira checker and api modules",
    license = "GPLv3",
    keywords = "moira graphite alert monitoring",
    url = "https://github.com/moira-alert",
    packages = find_packages(exclude=['tests']),
    long_description = 'Please, visit moira.readthedocs.org for more information',
    classifiers = [
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    ],
    entry_points = {
        'console_scripts': ['moira-api = moira.api.server:run',
                            'moira-checker = moira.checker.server:run',
                            'moira-update = moira.tools.converter:run'],
    },
    data_files = [
        ('/usr/lib/systemd/system', ['pkg/moira-api.service', 'pkg/moira-checker.service']),
        ('/etc/moira/', ['pkg/worker.yml']),
    ],
    install_requires = required,
)
