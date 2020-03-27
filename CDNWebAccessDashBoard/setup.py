from setuptools import setup

setup(name="pyflink-simple-dashboard",
      version="0.3",
      packages=['pyflink_dashboard', 'pyflink_dashboard.chart'],
      include_package_data=True,
      scripts=['pyflink_dashboard/start_dashboard.py'],
      package_data={'pyflink_dashboard.chart': ['*.js', '*.html']},
      install_requires=['cryptography', 'pymysql', 'kafka-python'],
      zip_safe=False)
