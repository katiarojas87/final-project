from setuptools import find_packages
from setuptools import setup

with open("requirements.txt") as f:
    content = f.readlines()
requirements = [x.strip() for x in content if "git+" not in x]

setup(name='final_project_package',
      version="0.0.1",
      description="FinalProject Model (using clip to predict price)",
      author="Katia, Lance and Bea",
      #author_email="contact@lewagon.org",
      #url="https://github.com/katiarojas87/final-project",
      install_requires=requirements,
      packages=find_packages(),
      #test_suite="tests",
      zip_safe=False)
