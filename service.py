import os
import sys

import logging
import yaml

from app.containers import Core, Services


if __name__ == '__main__':
    _, service_name, *args = sys.argv
    logging.basicConfig(
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    config_path = os.path.join(
        'config',
        'services',
        service_name,
        'config.yaml',
    )

    Core.config.update(yaml.load(open(config_path)))

    service = getattr(Services, service_name)()

    service.run()
