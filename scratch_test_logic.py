import re
from typing import List, Dict, Any, Optional, Tuple, Set
from app.database.connection import get_engine
from app.database.metadata import extract_database_metadata
from app.models.schema import DatabaseSchemaMetadata
from app.models.migration import ColumnMappingResult

def normalize_string(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    return re.sub(r'[^a-z0-9]', '', s.lower())

def stem_token(t: str) -> str:
    t = t.lower()
    if len(t) <= 3:
        return t
    if t.endswith("ies") and len(t) > 4:
        return t[:-3] + "y"
    if t.endswith("es") and len(t) > 3 and not t.endswith(("ss", "us", "is")):
        return t[:-2]
    if t.endswith("s") and len(t) > 3 and not t.endswith(("ss", "us", "is")):
        return t[:-1]
    return t

def tokenize(s: str) -> List[str]:
    if not s:
        return []
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    tokens = [t.lower() for t in re.split(r'[^a-zA-Z0-9]+', s) if t]
    return [stem_token(t) for t in tokens]

SYNONYM_CANONICAL = {
    # Outlets
    "restaurant": "outlet",
    "branch": "outlet",
    "store": "outlet",
    "shop": "outlet",
    # Orders / Bills
    "invoice": "bill",
    "invoiceno": "billno",
    "bill": "bill",
    "billno": "billno",
    "order": "order",
    "sale": "order",
    # Items
    "item": "item",
    "prod": "item",
    "product": "item",
    "dish": "item",
    "menu": "item",
    # Customers
    "cust": "customer",
    "customer": "customer",
    "client": "customer",
    # Contacts
    "phone": "contact",
    "phoneno": "contact",
    "phonenumber": "contact",
    "mobile": "contact",
    "mobileno": "contact",
    "tel": "contact",
    "contact": "contact",
    "contactno": "contact",
    # Quantities
    "qty": "quantity",
    "quantity": "quantity",
    "count": "quantity",
    # Prices / Amounts
    "rate": "price",
    "cost": "price",
    "unitprice": "price",
    "price": "price",
    "amount": "amount",
    "amt": "amount",
    "total": "amount",
    "subtotal": "amount",
    "gross": "amount",
    "myamount": "amount",
    # Charges / Taxes / Discounts
    "tax": "tax",
    "vat": "tax",
    "gst": "tax",
    "totaltax": "tax",
    "disc": "discount",
    "discount": "discount",
    "charge": "charge",
    "charges": "charge",
    "containercharge": "othercharge",
    "additionalcharge": "othercharge",
    "deliverycharge": "othercharge",
    "deductioncharge": "othercharge",
    "servicecharge": "servicecharge",
    # Descriptions / Names / Status
    "desc": "description",
    "description": "description",
    "name": "name",
    "status": "status",
    "orderstatus": "status",
    "reason": "reason",
    "cancelreason": "reason",
    "category": "category",
    "cat": "category",
    "date": "date",
    "datetime": "datetime",
    "time": "datetime",
    "address": "address",
    "addr": "address",
}

PRIMARY_OPERATIONAL_TABLES = {
    "customer", "order", "bills", "orderlineitems", "outlets", "itemsmaster", "item_groups", "settlements"
}

ARCHIVE_PATTERNS = [
    re.compile(r"^discarded", re.I),
    re.compile(r"^reinstate", re.I),
    re.compile(r"^kiosk_", re.I),
    re.compile(r"_tracking$", re.I),
    re.compile(r"^cloud_sync", re.I),
    re.compile(r"audit", re.I),
    re.compile(r"history", re.I),
    re.compile(r"backup", re.I),
    re.compile(r"archive", re.I),
    re.compile(r"temp", re.I),
    re.compile(r"sysdiagrams", re.I),
]

def is_archive_table(name: str) -> bool:
    return any(p.search(name) for p in ARCHIVE_PATTERNS)

def canonicalize_tokens(tokens: List[str]) -> Set[str]:
    return set(SYNONYM_CANONICAL.get(t, t) for t in tokens)

def run_test():
    import json
    engine = get_engine()
    schema = extract_database_metadata(engine)
    with open('uploads/upload_9fb1ee74487f_session.json') as f:
        session = json.load(f)
    excel_columns = session['columns']

    db_entries = []
    for table in schema.tables:
        t_tokens = canonicalize_tokens(tokenize(table.name))
        norm_t = normalize_string(table.name)
        norm_t_stem = normalize_string(stem_token(table.name))
        is_archive = is_archive_table(table.name)
        is_primary = table.name.lower() in PRIMARY_OPERATIONAL_TABLES

        for col in table.columns:
            c_tokens = canonicalize_tokens(tokenize(col.name))
            norm_c = normalize_string(col.name)
            is_pct = any(k in norm_c for k in ("percent", "rate", "pct"))
            is_pk = col.primary_key
            is_identity = getattr(col, "identity", False)

            db_entries.append({
                "table_name": table.name,
                "column_name": col.name,
                "table_tokens": t_tokens,
                "col_tokens": c_tokens,
                "combined_tokens": t_tokens | c_tokens,
                "norm_table": norm_t,
                "norm_table_stem": norm_t_stem,
                "norm_col": norm_c,
                "is_pk": is_pk,
                "is_identity": is_identity,
                "is_pct": is_pct,
                "is_archive": is_archive,
                "is_primary": is_primary,
                "col_type": col.type,
            })

    results = []
    for excel_col in excel_columns:
        clean_col = str(excel_col).strip()
        norm_e = normalize_string(clean_col)
        e_tokens = canonicalize_tokens(tokenize(clean_col))

        is_source_amount = any(w in norm_e for w in ("amount", "tax", "charge", "price", "discount", "total", "fee", "cost"))
        is_source_pct = any(w in norm_e for w in ("percent", "rate", "pct")) or "%" in clean_col
        is_source_reason = any(w in norm_e for w in ("reason", "cancel", "comment", "remark", "note", "desc"))

        scored = []
        for entry in db_entries:
            # Skip archive tables unless no primary match
            archive_penalty = -0.35 if entry["is_archive"] else 0.0
            primary_bonus = 0.08 if entry["is_primary"] else 0.0

            # Disallow amount to percentage rate
            if is_source_amount and entry["is_pct"] and not is_source_pct:
                continue

            # Heavy penalty if reason matched to amount or ID
            if is_source_reason and (any(w in entry["norm_col"] for w in ("amount", "price", "tax", "charge", "id", "count", "qty", "no")) or entry["is_pk"]):
                continue

            # Heavy penalty if descriptor name/phone matched to integer PK/ID
            if any(w in norm_e for w in ("name", "phone", "address")) and (entry["is_pk"] or entry["is_identity"] or entry["norm_col"].endswith("id")):
                continue

            score = 0.0
            m_type = "NONE"

            # 1. Exact Column Name Match
            if norm_e and norm_e == entry["norm_col"]:
                score, m_type = 1.0, "EXACT"

            # 2. Compound Match (e.g. restaurant_name -> Outlets.Outlet_Name or Customer.Customer_Name)
            elif norm_e and (
                norm_e in (f"{entry['norm_table']}{entry['norm_col']}", f"{entry['norm_col']}{entry['norm_table']}")
                or norm_e in (f"{entry['norm_table_stem']}{entry['norm_col']}", f"{entry['norm_col']}{entry['norm_table_stem']}")
            ):
                score, m_type = 0.98, "EXACT_COMPOUND"

            else:
                col_overlap = len(e_tokens & entry["col_tokens"])
                table_overlap = len(e_tokens & entry["table_tokens"])

                # Semantic exact match (tokens match canonical tokens exactly)
                if e_tokens and e_tokens == entry["col_tokens"]:
                    score, m_type = 0.95, "SEMANTIC_EXACT"

                # Semantic compound
                elif e_tokens and (e_tokens == entry["combined_tokens"] or (table_overlap > 0 and col_overlap == len(entry["col_tokens"]))):
                    score, m_type = 0.93, "SEMANTIC_COMPOUND"

                # POS Specific Concept alignments
                elif norm_e == "invoiceno" and entry["norm_col"] in ("billno", "billnumber"):
                    score, m_type = 0.94, "SEMANTIC_DOMAIN"
                elif norm_e == "restaurantname" and entry["norm_col"] == "outletname":
                    score, m_type = 0.96, "SEMANTIC_DOMAIN"
                elif norm_e in ("myamount", "total") and entry["norm_col"] in ("billamount", "price", "finalamount"):
                    score, m_type = 0.92, "SEMANTIC_DOMAIN"
                elif norm_e == "totaltax" and entry["norm_col"] in ("tax", "vat"):
                    score, m_type = 0.92, "SEMANTIC_DOMAIN"
                elif norm_e in ("containercharge", "additionalcharge", "deliverycharge", "deductioncharge") and entry["norm_col"] in ("othercharges", "other_charges"):
                    score, m_type = 0.90, "SEMANTIC_DOMAIN"
                elif norm_e == "servicecharge" and entry["norm_col"] in ("servicechargetotal", "othercharges", "other_charges"):
                    score, m_type = 0.91, "SEMANTIC_DOMAIN"
                elif norm_e == "itemquantity" and entry["norm_col"] == "quantity":
                    score, m_type = 0.95, "SEMANTIC_DOMAIN"
                elif norm_e == "itemname" and entry["norm_col"] in ("description", "item"):
                    score, m_type = 0.93, "SEMANTIC_DOMAIN"
                elif norm_e == "customerphone" and entry["norm_col"] in ("mobilenumber", "contactnumber"):
                    score, m_type = 0.94, "SEMANTIC_DOMAIN"
                elif norm_e == "customeraddress" and entry["norm_col"] == "address":
                    score, m_type = 0.95, "SEMANTIC_DOMAIN"
                elif norm_e == "date" and entry["norm_col"] in ("orderdatetime", "billdate"):
                    score, m_type = 0.90, "SEMANTIC_DOMAIN"
                elif norm_e == "ordercancelreason" and entry["norm_col"] in ("servicefailreason", "customercomments", "discardreason"):
                    score, m_type = 0.88, "SEMANTIC_DOMAIN"
                elif col_overlap > 0:
                    jaccard = col_overlap / max(len(e_tokens), len(entry["col_tokens"]))
                    score = 0.70 + (0.20 * jaccard) + (0.10 if table_overlap > 0 else 0.0)
                    m_type = "TOKEN_OVERLAP"

            # Apply archive penalty & primary bonus
            adjusted_score = score + archive_penalty + primary_bonus
            if entry["is_identity"] and adjusted_score < 1.0:
                adjusted_score -= 0.10

            if adjusted_score >= 0.75:
                scored.append((adjusted_score, entry, m_type))

        scored.sort(key=lambda x: x[0], reverse=True)
        if not scored:
            results.append((clean_col, "UNKNOWN", "UNKNOWN", 0.0, "UNMATCHED", []))
            continue

        best_score, best_entry, best_type = scored[0]
        # Ambiguity check: candidates within 0.02 of top score
        top_candidates = [s for s in scored if abs(s[0] - best_score) < 0.02]
        distinct_targets = {(s[1]["table_name"], s[1]["column_name"]) for s in top_candidates}

        if len(distinct_targets) > 1 and best_score < 0.98:
            candidates = [
                {"table": s[1]["table_name"], "col": s[1]["column_name"], "score": round(s[0], 2), "type": s[2]}
                for s in top_candidates
            ]
            results.append((clean_col, "UNKNOWN", "UNKNOWN", round(best_score, 2), "AMBIGUOUS", candidates))
        else:
            candidates = [
                {"table": s[1]["table_name"], "col": s[1]["column_name"], "score": round(s[0], 2), "type": s[2]}
                for s in scored[:4]
            ]
            results.append((clean_col, best_entry["table_name"], best_entry["column_name"], min(round(best_score, 2), 1.0), "MATCHED", candidates))

    for r in results:
        print(f"{r[0]:20} -> {r[1]:15}.{r[2]:20} [{r[4]}] ({r[3]})")

run_test()

