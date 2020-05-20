from setuptools import find_packages, setup

with open("requirements.txt") as f:
    requirements = f.read().splitlines()
with open("requirements_test.txt") as f:
    test_requirements = f.read().splitlines()

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    # Meta
    author="Clerk.ai",
    author_email="info@clerk.ai",
    description="Tools for extracting, annotating and summarizing transaction, location history and time tracking data from local files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    name="clerkai",
    license="mpl-2.0",
    url="https://github.com/clerkai/python-clerkai",
    version="0.1.1",
    download_url="https://github.com/clerkai/python-clerkai/archive/v0.1.1.tar.gz",
    packages=find_packages(),
    # Dependencies
    install_requires=requirements,
    tests_require=test_requirements,
    dependency_links=[
        "git+https://github.com/motin/pytest-annotate.git@allow-pytest-v4-and-v5#egg=pytest-annotate-1.0.2"
    ],
    setup_requires=["setuptools_scm", "pytest-runner"],
    # Packaging
    include_package_data=True,
    use_scm_version=False,
    zip_safe=False,
    # Classifiers
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Scientific/Engineering :: Image Recognition",
    ],
    python_requires='>=3.7',
)
