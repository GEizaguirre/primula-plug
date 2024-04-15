from distutils.core import setup
import os
import pathlib

from setuptools import find_packages


here = pathlib.Path(__file__).parent.resolve()

long_description = (here / "README.md").read_text(encoding="utf-8")

setup(name='primula',
      version='1.0',
      description='The Primula algorithm, as a generic pluggable predictor. ',
      long_description=long_description,
      long_description_content_type="text/markdown",
      author='Germán T. Eizaguirre',
      author_email='germantelmo.eizaguirre@urv.cat',
      url='https://github.com/GEizaguirre/primula-plug',
      packages=find_packages()
     )

home_dir = os.path.expanduser("~")
primula_home_dir = home_dir + "/.primula"
os.makedirs(primula_home_dir, exist_ok=True)
