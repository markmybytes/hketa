from pathlib import Path
from setuptools import find_packages, setup

with open(Path(__file__).parent.joinpath('README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='hketa',
    version='0.0.3',
    description='A Python package that provides normalised data access to public transports in Hong Kong.',
    long_description_content_type='text/markdown',
    long_description=long_description,
    url="https://github.com/SuperDumbTM/hketa",
    install_requires=[
        'aiohttp',
        'beautifulsoup4',
        'pyproj',
        'pytz'
    ],
    packages=find_packages(where='src'),
    package_dir={'': 'src'}
)
