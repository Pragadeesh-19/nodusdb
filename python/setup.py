from setuptools import setup
from setuptools.command.bdist_wheel import bdist_wheel
from setuptools.dist import Distribution


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


class PlatformWheel(bdist_wheel):
    def get_tag(self):
        _, _, platform = super().get_tag()
        return "py3", "none", platform


setup(distclass=BinaryDistribution, cmdclass={"bdist_wheel": PlatformWheel})
