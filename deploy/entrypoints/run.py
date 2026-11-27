"""Load explicitly mounted file secrets and replace this process."""
import os
from pathlib import Path
import sys

FILES = {
    'SECRET_KEY': 'SECRET_KEY_FILE',
    'MYSQL_PASSWORD': 'MYSQL_PASSWORD_FILE',
    'ONECITY_RSA_PRIVATE_KEY_PASSPHRASE': 'ONECITY_RSA_PRIVATE_KEY_PASSPHRASE_FILE',
    'CHAT_MODEL_API_KEY': 'CHAT_MODEL_API_KEY_FILE',
    'DEER_FLOW_ADMISSION_SIGNING_KEY': 'DEER_FLOW_ADMISSION_SIGNING_KEY_FILE',
    'ES_PASSWORD': 'ES_PASSWORD_FILE',
    'ADMIN_PASSWORD': 'ADMIN_PASSWORD_FILE',
}
for variable, file_variable in FILES.items():
    filename = os.environ.get(file_variable)
    if filename:
        value = Path(filename).read_text().strip()
        if value:
            os.environ[variable] = value
if not sys.argv[1:]:
    raise SystemExit('No service command supplied')
os.execvp(sys.argv[1], sys.argv[1:])
