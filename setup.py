from distutils.core import setup
import os


setup(name='primula',
      version='1.0',
      description='The Primula algorithm, as a generic pluggable predictor. ',
      author='Germán Telmo Eizaguirre',
      author_email='germantelmo.eizaguirre@urv.cat',
      url='https://github.com/GEizaguirre/primula-plug',
      packages=['primula']
     )

home_dir = os.path.expanduser("~")
primula_home_dir = home_dir + "/.primula"
os.makedirs(primula_home_dir, exist_ok=True)
