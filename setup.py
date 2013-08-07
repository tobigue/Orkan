from distutils.core import setup

setup(
    name='Orkan',
    version='0.2.1',
    author='Tobias Guenther',
    author_email='orkan@tobias.io',
    packages=['orkan', 'orkan.test'],
    scripts=[],
    url='https://github.com/tobigue/Orkan',
    license='LICENSE.txt',
    description='Orkan is a pipeline parallelization library, written in Python.',
    long_description=open('README.rst').read(),
    install_requires=[
        "futures"
    ],
)
