from setuptools import setup, find_packages

setup(
    name = 'monoconnecttest',
    version = '0.1.0',
    url = 'https://github.com/naveengulati0207/mongoconnecttest.git',
    description = 'this is for testin purpose',
    packages = find_packages(),
    install_requires = [
        # Github Private Repository
        'monoconnecttest @ git+ssh://git@github.com/example_org/monoconnecttest.git#egg=monoconnecttest-0.1'
    ]
)
