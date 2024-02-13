import pandas
import json
import datetime
import pytz

ibd = pandas.read_excel('file:///Users/bob/Documents/ibd/IBD 50.xls')
found_hdr = False
column_hdr = None
tz = pytz.timezone('GMT')

attr_name_map = {
    'Symbol': 'tickerSymbol',
    'Company Name': 'companyName',
    'Composite Rating': 'compositeRating',
    'EPS Rating': 'epsRating',
    'RS Rating': 'relativeStrength',
    'SMR Rating': 'salesMarginRoe',
    'ACC/DIS Rating': 'accumDist',
    'Group Rel Str Rating': 'groupStrength',
    'Mgmt Own %': 'mgmtOwnPct'
}
attr_name_values = attr_name_map.keys()
print(attr_name_values)

ibd_list = []

for item in ibd.values:
    ibd_item = {}
    if isinstance(item[0], str):
        if item[0] == 'Symbol':
            column_hdr = item
            found_hdr = True
        else:
            if found_hdr:
                for idx, col_name in enumerate(column_hdr):
                    if col_name in attr_name_values:
                        ibd_item[attr_name_map[col_name]] = item[idx]
                    else:
                        continue
                ibd_item['createDate'] = datetime.datetime.now(tz).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
                ibd_list.append(ibd_item)
    else:
        if found_hdr:
            break

for item in ibd_list:
    print(json.dumps(item, indent=4))
