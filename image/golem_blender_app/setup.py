from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()


setup(
    name='Golem-Blender-App',
    version='0.1.0',
    url='https://github.com/golemfactory/blenderapp',
    maintainer='The Golem team',
    maintainer_email='tech@golem.network',
    packages=[
        'golem_blender_app',
        'golem_blender_app.commands',
        'golem_blender_app.render_tools',
        'golem_blender_app.verifier_tools',
    ],
    include_package_data=True,
    data_files=[
        ('render_tools/templates',
         ['golem_blender_app/render_tools/templates/blendercrop.py.template']),
        ('verifier_tools',
         ['golem_blender_app/verifier_tools/tree35_[crr=87.71][frr=0.92].pkl']),
    ],
    python_requires='>=3.6',
    install_requires=requirements,
)
