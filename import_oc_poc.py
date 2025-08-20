#!/usr/bin/env python3
"""
PoC: leer CSV de OCs, validar, y crear purchase.order en Odoo via JSON-RPC.
Adaptar ENDPOINT, AUTH y model calls según la instalación Odoo.
"""

import csv
import json
import requests
import time
from datetime import datetime
from decimal import Decimal

# CONFIG - rellenar con datos de la empresa (staging)
ODOO_URL = "https://odoo.staging.example.com/jsonrpc"
API_TOKEN = "TU_TOKEN_STAGING"
HEADERS = {"Content-Type": "application/json", "Authorization": f"Bearer {API_TOKEN}"}

INPUT_CSV = "ocs_example.csv"
PROCESSED_FOLDER = "processed/"
QUARANTINE_FOLDER = "quarantine/"

# --- Helpers JSON-RPC
def odoo_rpc(method, params):
    payload = {"jsonrpc":"2.0","method":"call","params":params,"id":int(time.time())}
    r = requests.post(ODOO_URL, json=payload, headers=HEADERS, timeout=30)
    r.raise_for_status()
    resp = r.json()
    if "error" in resp:
        raise Exception(resp["error"])
    return resp.get("result")

# Buscar partner por RFC (vat)
def buscar_partner_por_rfc(rfc):
    params = {"model":"res.partner","method":"search_read","args":[[["vat","=",rfc]], ["id","name","vat"]], "kwargs":{"limit":1}}
    res = odoo_rpc("call", params)
    return res[0] if res else None

# Buscar producto por default_code
def buscar_producto_por_codigo(code):
    params = {"model":"product.product","method":"search_read","args":[[["default_code","=",code]]], "kwargs":{"limit":1}}
    res = odoo_rpc("call", params)
    return res[0] if res else None

# Check duplicado por origin
def oc_existe(origin):
    params = {"model":"purchase.order","method":"search_read","args":[[["origin","=",origin]]], "kwargs":{"limit":1}}
    res = odoo_rpc("call", params)
    return len(res) > 0

# Crear purchase.order
def crear_purchase_order(partner_id, origin, date_order, order_lines, currency_id=None, project_id=None):
    vals = {
        "partner_id": partner_id,
        "origin": origin,
        "date_order": date_order,
        "order_line": [[0,0,line] for line in order_lines]
    }
    if currency_id: vals["currency_id"] = currency_id
    if project_id: vals["project_id"] = project_id
    params = {"model":"purchase.order","method":"create","args":[vals]}
    res = odoo_rpc("call", params)
    return res  # id de la orden creada

# Validaciones por fila
def validar_fila(row):
    errors = []
    try:
        if not row["oc_number"].strip(): errors.append("oc_number vacío")
        qty = Decimal(row["cantidad"])
        if qty <= 0: errors.append("cantidad <= 0")
        price = Decimal(row["precio_unitario"])
        if price < 0: errors.append("precio_unitario negativo")
        # parse fecha
        datetime.strptime(row["fecha"], "%Y-%m-%d")
    except Exception as e:
        errors.append(f"validación formato: {e}")
    return errors

# Procesar CSV
def procesar_csv(path):
    processed = []
    quarantined = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            row_errors = validar_fila(row)
            if row_errors:
                row["error"] = "; ".join(row_errors)
                quarantined.append(row)
                continue

            origin = row["oc_number"].strip()
            if oc_existe(origin):
                row["error"] = "DUPLICATE"
                quarantined.append(row)
                continue

            # partner
            partner = buscar_partner_por_rfc(row["proveedor_rfc"])
            if not partner:
                # crear partner provisional? aquí lo marcamos
                row["error"] = "PROVIDER_NOT_FOUND"
                quarantined.append(row)
                continue

            product = buscar_producto_por_codigo(row["item_code"])
            if not product:
                row["error"] = "PRODUCT_NOT_FOUND"
                quarantined.append(row)
                continue

            # construir line
            line = {
                "product_id": product["id"],
                "name": row["descripcion"],
                "product_qty": float(row["cantidad"]),
                "price_unit": float(row["precio_unitario"]),
                # "taxes_id": [ (6,0,[tax_id]) ]  # ejemplo si aplica
            }

            try:
                order_id = crear_purchase_order(partner["id"], origin, row["fecha"], [line])
                row["created_id"] = order_id
                processed.append(row)
            except Exception as e:
                row["error"] = f"API_ERROR:{str(e)}"
                quarantined.append(row)
    return processed, quarantined

if __name__ == "__main__":
    p,q = procesar_csv(INPUT_CSV)
    # grabar logs
    import csv
    if p:
        with open(PROCESSED_FOLDER + "processed.csv", "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(p[0].keys()))
            writer.writeheader()
            writer.writerows(p)
    if q:
        with open(QUARANTINE_FOLDER + "quarantine.csv", "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(q[0].keys()))
            writer.writeheader()
            writer.writerows(q)
    print(f"OK processed: {len(p)}, quarantined: {len(q)}")
