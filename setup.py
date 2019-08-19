from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()
with open('requirements_test.txt') as f:
    test_requirements = f.read().splitlines()

setup(
    # Meta
    author='Clerk.ai Developers',
    author_email='info@clerk.ai',
    description='Tools for extracting transaction, receipt, travel and time information from local files',
    name='clerkai',
    license='ISC',
    url='https://github.com/clerk/python-clerkai',
    version='0.1.0',
    packages=find_packages(),

    # Dependencies
    install_requires=requirements,
    tests_require=test_requirements,
    setup_requires=['setuptools_scm', 'pytest-runner'],

    # Packaging
    include_package_data=True,
    use_scm_version=False,
    zip_safe=False,

    # Classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'Topic :: Scientific/Engineering :: Image Recognition',
    ],
)
