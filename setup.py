from setuptools import setup

url = ""
version = "0.2.4"
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
    download_url="{}/tarball/{}".format(url, version),
    license="MIT"
)
