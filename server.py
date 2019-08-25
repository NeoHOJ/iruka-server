import sys
from pathlib import Path

# export iruka client+protos to module search path
base_path = Path(__file__).parent.absolute()
sys.path.insert(0, str(base_path / 'iruka_client'))

import iruka_server.cli

iruka_server.cli.main()
