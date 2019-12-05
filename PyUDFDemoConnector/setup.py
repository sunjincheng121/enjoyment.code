from setuptools import setup

setup(name="pyflink-demo-connector",
      version="0.1",
      packages=['pyflink.demo', 'pyflink.lib', 'pyflink.demo.chart'],
      include_package_data=True,
      package_dir={'pyflink.lib': 'target'},
      package_data={'pyflink.lib': ['*.jar'], 'pyflink.demo.chart': ['*.js', '*.html']},
      zip_safe=False)
