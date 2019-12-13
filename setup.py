from setuptools import setup

url = ""
version = "0.1.0"
readme = open('README.rst').read()

setup(
    name="pyrocoto",
    packages=["pyrocoto"],
    version=version,
    description="Python API for creating validated rocoto xml definitions",
    long_description=readme,
    include_package_data=True,
    author="Adam Schnapp",
    author_email="adschnapp@gmail.com",
    url=url,
    install_requires=['pyyaml'],
    download_url="{}/tarball/{}".format(url, version),
    license="MIT"
)
