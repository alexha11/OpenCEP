from setuptools import setup, find_packages

setup(
    name="opencep",
    version="0.1.0",
    packages=find_packages(),
    python_requires='>=3.6',
    install_requires=[
        'pandas',
        'numpy',
        'matplotlib',
        'seaborn'
    ],
    author="OpenCEP Contributors",
    description="Complex Event Processing library",
)