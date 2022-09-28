import setuptools

setuptools.setup(
    name="data_loader",
    description="data loader library",
    packages=setuptools.find_packages(include=["data_reader", "data_reader.*"])
)