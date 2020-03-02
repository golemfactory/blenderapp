#!/usr/bin/env python3
from setuptools import setup

from golem_blender_app import constants


def parse_requirements():
    """
    Parse requirements.txt file
    Taken from https://github.com/golemfactory/golem/blob/0.20.1/setup_util/setup_commons.py#L223
    :return: [requirements, dependencies]
    """
    import re
    requirements = []
    dependency_links = []
    for line in open('requirements.txt'):
        line = line.strip()
        if line.startswith('-') or line.startswith('#'):
            continue

        m = re.match('.+#egg=(?P<package>.+?)(?:&.+)?$', line)
        if m:
            requirements.append(m.group('package'))
            dependency_links.append(line)
        else:
            requirements.append(line)
    return requirements, dependency_links

install_requires, dependencies = parse_requirements()

setup(
    name='Golem-Blender-App',
    version=constants.VERSION,
    url='https://github.com/golemfactory/blenderapp',
    maintainer='The Golem team',
    maintainer_email='tech@golem.network',
    packages=[
        'golem_blender_app',
        'golem_blender_app.commands',
        'golem_blender_app.render_tools',
        'golem_blender_app.verifier_tools',
        'golem_blender_app.verifier_tools.file_extension',
        'golem_blender_app.process_tools',
    ],
    include_package_data=True,
    data_files=[
        ('render_tools/templates',
         ['golem_blender_app/render_tools/templates/blendercrop.py.template']),
        ('verifier_tools',
         ['golem_blender_app/verifier_tools/tree35_[crr=87.71][frr=0.92].pkl']),
    ],
    python_requires='>=3.6',
    install_requires=install_requires,
    dependency_links=dependencies,
)
