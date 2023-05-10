import json
from datetime import datetime

today = datetime.today()

month = today.month
year = today.year

with open("src/bcsd_parameter_template.json") as json_file:
    parameter = json.load(json_file)
    parameter["issue_date"]["month"] = month
    parameter["issue_date"]["year"] = year

    json.dump(parameter, open("src/bcsd_parameter.json", "w"), indent=4)
