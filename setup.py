from setuptools import setup

setup(
    name="flcac_utils",
    version="0.2.1",
    packages=["flcac_utils"],
    include_package_data=True,
    python_requires=">=3.9",
    install_requires=["fedelemflowlist @ git+https://github.com/USEPA/fedelemflowlist.git@develop#egg=fedelemflowlist",
                      "esupy @ git+https://github.com/USEPA/esupy.git@develop#egg=esupy",
                      "olca-schema>=2.2",
                      "bibtexparser<2.0",
                      "pandas>=2.2",
                      "numpy>=2.1",
                      "pyyaml>=5.3"
                      ],
    # description=''
)
