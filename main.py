import subprocess
import sys


def install_packages():
    print("Installing packages...")
    with open('requirements.txt', 'r') as requirements:
        packages = requirements.read().splitlines()
        for package in packages:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
    print(f"Packages {packages} installed successfully")


install_packages()

import final_project

final_project.run()
