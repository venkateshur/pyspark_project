from setuptools import setup, find_namespace_packages

setup(name="pyspark_join_package",
      version="1.0",
      package_dir={'': 'src'},
      packages=find_namespace_packages(where='src'),
      install_requires=['docutils>=0.3'],

      package_data={
          # If any package contains *.txt or *.rst files, include them:
          '': ['*.txt', '*.rst'],
          # And include any *.msg files found in the 'hello' package, too:
          'hello': ['*.msg']},
      author="Me",
      author_email="me@example.com",
      description="Py Spark join example",
      keywords="pyspark join examples"  # project home page, if any
      )
