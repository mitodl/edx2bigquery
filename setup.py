import glob
from pathlib import Path
from setuptools import setup

# read the contents of README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

def findfiles(pat):
    return [x for x in glob.glob('share/' + pat)]

data_files = [
#    ('share/render', findfiles('render/*')),
    ]

# print "data_files = %s" % data_files

setup(
    name='edx2bigquery',
    version='1.3.0',
    author='I. Chuang',
    author_email='ichuang@mit.edu',
    packages=['edx2bigquery', 'edx2bigquery.test'],
    scripts=[],
    url='https://github.com/mitodl/edx2bigquery',
    license='LICENSE',
    description='Import research data from edX dumps into google BigQuery',
    long_description=long_description,
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'edx2bigquery = edx2bigquery.main:CommandLine',
            ],
        },
    install_requires=['path.py',
                      'argparse',
		      'pygeoip',
                      'pytz',
                      'python-dateutil',
                      'geoip2',
                      'lxml',
                      # 'BeautifulSoup',
                      'bs4',
                      'unicodecsv',
                      'Jinja2',
                      'google-api-python-client',
                      'edxcut',
                      'unidecode',
                      ],
    dependency_links = [
        ],
    package_dir={'edx2bigquery': 'edx2bigquery'},
    package_data={'edx2bigquery': ['lib/*', 'bin/*'] },
    # package_data={ 'edx2bigquery': ['python_lib/*.py'] },
    # data_files = data_files,
    test_suite = "edx2bigquery.test",
)
