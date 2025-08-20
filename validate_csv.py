import csv
from datetime import datetime
from decimal import Decimal

INPUT = "ocs_example.csv"
VALID_OUT = "valid.csv"
QUAR_OUT = "quarantine.csv"

def is_date(s):
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except:
        return False

def is_decimal(s):
    try:
        Decimal(s)
        return True
    except:
        return False

valid = []
quar = []
with open(INPUT,newline='',encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for r in reader:
        errs = []
        if not r['oc_number'].strip(): errs.append("OC_EMPTY")
        if not is_date(r['fecha']): errs.append("BAD_DATE")
        if not r['proveedor_rfc'].strip(): errs.append("RFC_EMPTY")
        if not is_decimal(r['cantidad']) or float(r['cantidad'])<=0: errs.append("BAD_QTY")
        if not is_decimal(r['precio_unitario']) or float(r['precio_unitario'])<0: errs.append("BAD_PRICE")
        # check total if present
        if 'total' in r and r['total'].strip():
            if round(float(r['cantidad'])*float(r['precio_unitario']),2) != round(float(r['total']),2):
                errs.append("TOTAL_MISMATCH")
        if errs:
            r['error'] = ";".join(errs)
            quar.append(r)
        else:
            valid.append(r)

# grabar
if valid:
    with open(VALID_OUT,"w",newline='',encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(valid[0].keys()))
        writer.writeheader()
        writer.writerows(valid)
if quar:
    with open(QUAR_OUT,"w",newline='',encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(quar[0].keys()))
        writer.writeheader()
        writer.writerows(quar)
print(f"valid: {len(valid)}, quarantined: {len(quar)}")
