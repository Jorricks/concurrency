from setuptools import setup

test_dependencies = [
]

lint_dependencies = ["flake8==3.7.9"]

setup(
    name="ConcurrierApi",
    version="1.0.0",
    packages=["src.concurrier"],
    include_package_data=True,
    entry_points={"console_scripts": ["ConcurrierApi = src.concurrier.__main__:main"]},
    install_requires=[
        "dataclasses-json==0.4.2",
        "fastapi==0.54.1",
        "pydantic==1.5.1",
        "redis==3.5.0",
    ],
    extras_require={
        "test": test_dependencies,
        "lint": lint_dependencies,
        "dev": test_dependencies + lint_dependencies
    },
)
