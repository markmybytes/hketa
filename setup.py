from setuptools import find_packages, setup

setup(
    name='hketa',
    version='0.0.1',
    install_requires=[
        'aiohttp',
        'pytz'
    ],
    packages=find_packages(where='src'),
    package_dir={'': 'src'}
)
