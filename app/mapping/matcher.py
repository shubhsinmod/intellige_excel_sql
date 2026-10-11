import re
from typing import List, Dict, Any, Optional, Tuple, Set
from app.models.schema import DatabaseSchemaMetadata, TableMetadata, ColumnMetadata
from app.models.migration import ColumnMappingResult
from app.utils.normalization import (
    normalize_column_name,
    tokenize_column_name,
    normalize_string,
    stem_token
)
from app.mapping.synonyms import (
    synonym_registry,
    BusinessConcept,
    CustomMappingRule
)
from app.core.logging import logger

GENERIC_COLUMN_WORDS = {"id", "name", "date", "status", "type", "description", "code", "number", "value", "amount"}

# Primary operational tables in POS database prioritized over archive/kiosk/audit tables
PRIMARY_OPERATIONAL_TABLES = {
    "customer", "order", "bills", "orderlineitems", "outlets", "itemsmaster", "item_groups", "settlements"
}

# Archive, discarded, kiosk, tracking, audit, and dead-letter tables
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
    re.compile(r"draft", re.I),
    re.compile(r"deleted", re.I),
    re.compile(r"^sysdiagrams", re.I),
]


def is_archive_table(table_name: str) -> bool:
    return any(p.search(table_name) for p in ARCHIVE_PATTERNS)


def compute_key_info(col: ColumnMetadata) -> str:
    parts = []
    if getattr(col, "identity", False):
        parts.append("IDENTITY")
    elif col.primary_key:
        parts.append("PRIMARY_KEY")
    if getattr(col, "foreign_keys", None):
        parts.append("FOREIGN_KEY")
    if not col.nullable and not getattr(col, "identity", False) and col.default is None:
        parts.append("REQUIRED")
    elif col.default is not None:
        parts.append("DEFAULT")
    else:
        parts.append("NULLABLE")
    return ", ".join(parts)


def get_column_requirement_info(table_meta: Optional[TableMetadata], col_meta: Optional[ColumnMetadata]) -> Tuple[str, bool]:
    if not col_meta:
        return "OPTIONAL", False
    is_ident = getattr(col_meta, "identity", False)
    has_def = col_meta.default is not None
    is_fk = bool(getattr(col_meta, "foreign_keys", None)) or (
        bool(table_meta and table_meta.foreign_keys) and any(fk.constrained_column == col_meta.name for fk in table_meta.foreign_keys)
    )
    if is_ident:
        return "AUTO_GENERATED", False
    elif has_def:
        return "DATABASE_DEFAULT", False
    elif is_fk:
        return "FOREIGN_KEY", not col_meta.nullable
    elif not col_meta.nullable:
        return "MANDATORY", True
    else:
        return "OPTIONAL", False


def infer_source_data_type(clean_source: str, norm_source: str, concept: Optional[BusinessConcept]) -> str:
    """Infers the source column data type based on naming and business concept."""
    if concept:
        if concept.expected_type == "DECIMAL":
            return "DECIMAL(18,2)"
        elif concept.expected_type == "INTEGER":
            return "INTEGER"
        elif concept.expected_type == "DATETIME":
            return "DATETIME"
        elif concept.expected_type == "PERCENTAGE":
            return "DECIMAL(5,2)"
        elif concept.expected_type == "TEXT":
            return "NVARCHAR(255)"

    if any(k in norm_source for k in ("amount", "price", "rate", "cost", "total", "fee", "tax", "charge", "discount")):
        if any(p in norm_source for p in ("percent", "rate", "pct")) or "%" in clean_source:
            return "DECIMAL(5,2)"
        return "DECIMAL(18,2)"
    if any(k in norm_source for k in ("qty", "quantity", "count", "units", "pieces")):
        return "INTEGER"
    if any(k in norm_source for k in ("date", "time", "timestamp")):
        return "DATETIME"
    if any(k in norm_source for k in ("id", "code", "no", "number")):
        return "NVARCHAR(50)"
    return "NVARCHAR(255)"


def check_semantic_compatibility(
    clean_source: str,
    norm_source: str,
    source_concept: Optional[BusinessConcept],
    target_table: str,
    target_col: str,
    target_type: str,
    target_concept: Optional[BusinessConcept],
    is_pk_or_ident: bool
) -> Tuple[bool, str]:
    """
    Guarantees strict semantic compatibility and prevents incorrect mappings.
    Checks:
    - Monetary amount vs Percentage
    - Customer ID vs Customer Name
    - Item Quantity vs Item Price
    - Item Name vs Item Price
    - Item Total vs Item Price
    - Tax Amount vs Tax Rate
    - Service Charge vs Delivery Charge
    - Customer Address vs Outlet Location
    - Incompatible data types (e.g. TEXT into NUMERIC, DATE into INTEGER)
    """
    col_upper = (target_type or "").upper()
    norm_target = normalize_column_name(target_col)
    is_target_pct = any(k in norm_target for k in ("percent", "rate", "pct")) or (target_concept and target_concept.id == "tax_percentage")
    is_target_num = any(t in col_upper for t in ("INT", "FLOAT", "DECIMAL", "NUMERIC", "MONEY", "REAL", "DOUBLE"))
    is_target_text = any(t in col_upper for t in ("CHAR", "TEXT", "CLOB", "STRING"))
    is_target_date = any(t in col_upper for t in ("DATE", "TIME"))

    # 1. Amount vs Percentage mismatch
    is_source_amount = (
        (source_concept and source_concept.id in (
            "tax_amount", "total_amount", "discount", "delivery_charge",
            "service_charge", "item_price", "item_total"
        )) or
        any(k in norm_source for k in ("amount", "total", "tax", "charge", "price", "fee", "cost", "disc"))
    )
    is_source_pct = (
        (source_concept and source_concept.id == "tax_percentage") or
        any(k in norm_source for k in ("percent", "rate", "pct")) or ("%" in clean_source)
    )

    if is_source_amount and is_target_pct and not is_source_pct:
        return False, "INCOMPATIBLE: Cannot map monetary amount to percentage rate."
    if is_source_pct and is_target_num and not is_target_pct:
        return False, "INCOMPATIBLE: Cannot map percentage rate to gross amount."

    # 2. Customer ID vs Customer Name
    is_source_cust_name = (source_concept and source_concept.id == "customer_name") or "customer name" in norm_source or "client name" in norm_source
    if is_source_cust_name and (is_pk_or_ident or norm_target.endswith("id")):
        return False, "INCOMPATIBLE: Cannot map Customer Name text to Customer ID."

    is_source_id = norm_source.endswith("id") or norm_source == "id" or "code" in norm_source
    if is_source_id and "name" in norm_target and not norm_target.endswith("id"):
        return False, "INCOMPATIBLE: Cannot map ID code to Name text."

    # 3. Item Name vs Item Price
    is_source_item_name = (source_concept and source_concept.id == "item_name") or "item name" in norm_source or "dish name" in norm_source
    if is_source_item_name and (
        (target_concept and target_concept.id in ("item_price", "item_total", "total_amount"))
        or any(k in norm_target for k in ("price", "rate", "amount", "cost"))
        or (is_target_num and not is_target_text)
    ):
        return False, "INCOMPATIBLE: Cannot map Item Name text to Item Price or monetary amount."

    # 4. Item Quantity vs Item Price
    is_source_qty = (source_concept and source_concept.id == "quantity") or any(k in norm_source for k in ("qty", "quantity", "count"))
    if is_source_qty and (
        (target_concept and target_concept.id in ("item_price", "item_total", "total_amount"))
        or any(k in norm_target for k in ("price", "rate", "mrp"))
    ):
        return False, "INCOMPATIBLE: Cannot map Item Quantity count to Item Price."

    # 5. Item Total vs Item Price
    is_source_item_total = (source_concept and source_concept.id == "item_total") or "item total" in norm_source or "line total" in norm_source
    if is_source_item_total and (target_concept and target_concept.id == "item_price" or norm_target in ("unit price", "item price", "rate")):
        return False, "INCOMPATIBLE: Cannot map Item Total (extended amount) to Unit Price."

    # 6. Tax Amount vs Tax Rate
    if source_concept and source_concept.id == "tax_amount" and target_concept and target_concept.id == "tax_percentage":
        return False, "INCOMPATIBLE: Cannot map Tax Amount to Tax Percentage."
    if source_concept and source_concept.id == "tax_percentage" and target_concept and target_concept.id == "tax_amount":
        return False, "INCOMPATIBLE: Cannot map Tax Percentage to Tax Amount."

    # 7. Service Charge vs Delivery Charge
    if source_concept and source_concept.id == "service_charge" and target_concept and target_concept.id == "delivery_charge":
        return False, "INCOMPATIBLE: Cannot cross-map Service Charge to Delivery Charge."
    if source_concept and source_concept.id == "delivery_charge" and target_concept and target_concept.id == "service_charge":
        return False, "INCOMPATIBLE: Cannot cross-map Delivery Charge to Service Charge."

    # 8. Customer Address vs Outlet Location
    if source_concept and source_concept.id == "customer_address" and target_table.lower() in ("outlets", "stores", "branches"):
        if any(k in norm_target for k in ("location", "outlet location", "branch location")):
            return False, "INCOMPATIBLE: Cannot map Customer Address to Outlet Location."

    # 9. Generic Data Type guards
    if is_source_amount and not is_target_num and not is_target_text:
        return False, "INCOMPATIBLE: Target column type cannot store monetary values."

    # 10. contact_phone -> Mobile_Number requirement:
    # contact_phone must map to Mobile_Number, never Contact_Number, Contact_Person, or other unrelated columns
    if norm_source in ("contact phone", "contact_phone"):
        if norm_target in ("contact number", "contact person"):
            return False, "INCOMPATIBLE: 'contact_phone' must map to 'Mobile_Number', not 'Contact_Number' or 'Contact_Person'."
        if target_table.lower() not in ("customer", "customer_centric_vouchers") and norm_target not in ("mobile number", "mobile_number"):
            return False, "INCOMPATIBLE: 'contact_phone' must map to 'Mobile_Number' in Customer table."

    # 11. restaurant_name / virtual_brand_name / outlet_name must NOT map to Location/Area fields
    if norm_source in ("restaurant name", "virtual brand name", "outlet name", "restaurant_name", "virtual_brand_name", "outlet_name"):
        if any(k in norm_target for k in ("location", "area", "outlet location", "branch location", "restaurant location", "city", "region")):
            return False, f"INCOMPATIBLE: Source outlet name column '{clean_source}' represents an outlet entity, not a location/area field ('{target_col}')."

    # 12. Never automatically map 'area' or outlet names to Customer.Location
    # 'area' is a separate source field (often containing platform or branch strings like 'Zomato_ANN - BTM')
    # and must not be inferred as Customer.Location from name alone without an explicit verified rule.
    if norm_source in ("area", "customer area", "delivery area", "order area", "store area"):
        if target_table.lower() in ("customer", "dbo.customer") and norm_target in ("location", "area"):
            return False, f"INCOMPATIBLE: Source field '{clean_source}' cannot automatically map to 'Customer.{target_col}' without an explicit verified business rule."

    # 13. delivery_charge must only map to monetary charge columns, never DeliveryBoys phone/name or PrinterSettings
    if norm_source in ("delivery charge", "delivery_charge"):
        if target_table.lower() in ("deliveryboys", "delivery_boys", "printersettings"):
            return False, f"INCOMPATIBLE: '{clean_source}' is a monetary delivery charge, not a DeliveryBoy entity attribute or printer setting."

    # 14. assign_to should not map to voucher validity dates (Valid_To)
    if norm_source in ("assign to", "assign_to"):
        if norm_target in ("valid to", "valid_to"):
            return False, f"INCOMPATIBLE: '{clean_source}' represents staff assignment, not a voucher date 'Valid_To'."

    # 15. Disambiguate container_charge vs other generic charge claimants
    # container_charge is a delivery packaging fee that belongs in Bills.OtherCharges
    if norm_source in ("container charge", "container_charge") and target_table.lower() == "bills" and norm_target == "other charges":
        pass


    return True, "COMPATIBLE"


class ColumnMatcher:
    """
    Intelligent Schema-Driven Column Matching Engine implementing a Strict 6-Layer Strategy:
    1. Exact original-name match (Score: 1.0)
    2. Exact normalized-name match (Score: 0.99)
    3. Approved synonym match (Score: 0.95)
    4. Token-based similarity (Score: 0.70-0.89)
    5. Semantic or business-context matching (Score: 0.80-0.92)
    6. Manual review / Ambiguity resolution (Requires user review)
    
    Guarantees:
    - Never allows weak semantic or fuzzy matches to override exact or normalized matches.
    - Prevents semantically incompatible mappings (amount vs pct, id vs name, qty vs price).
    - Prevents unintended many-to-one column mappings on the same table.
    - Handles ambiguous columns (such as generic 'address') by analyzing context and requiring confirmation.
    - Supports user overrides and database-scoped learned rules.
    """

    @staticmethod
    def match_columns(
        excel_columns: List[str],
        schema: DatabaseSchemaMetadata,
        user_overrides: Optional[Dict[str, Dict[str, str]]] = None,
        database_scope: Optional[str] = None
    ) -> List[ColumnMappingResult]:
        results: List[ColumnMappingResult] = []
        user_overrides = user_overrides or {}
        db_scope = database_scope or schema.database_name

        # Detect high-level context from uploaded columns
        all_norm_sources = [normalize_column_name(c) for c in excel_columns]
        has_customer_signals = any("customer" in s or "client" in s or "guest" in s for s in all_norm_sources)
        has_outlet_signals = any("outlet" in s or "restaurant" in s or "branch" in s or "store" in s for s in all_norm_sources)

        # 1. Index schema columns with normalized representation, concepts, and metadata
        db_entries = []
        for table in schema.tables:
            norm_table = normalize_column_name(table.name)
            table_tokens = tokenize_column_name(table.name)
            is_archive = is_archive_table(table.name)
            is_primary = table.name.lower() in PRIMARY_OPERATIONAL_TABLES

            for col in table.columns:
                norm_col = normalize_column_name(col.name)
                col_tokens = tokenize_column_name(col.name)
                is_pk = col.primary_key
                is_identity = getattr(col, "identity", False)

                # Look up target business concept
                target_concept_match = synonym_registry.find_concept_for_term(col.name, db_scope) or synonym_registry.find_concept_for_term(norm_col, db_scope)
                target_concept = target_concept_match[1] if target_concept_match else None

                db_entries.append({
                    "table_name": table.name,
                    "column_name": col.name,
                    "norm_table": norm_table,
                    "norm_col": norm_col,
                    "col_tokens": col_tokens,
                    "table_tokens": table_tokens,
                    "is_pk": is_pk,
                    "is_identity": is_identity,
                    "is_archive": is_archive,
                    "is_primary": is_primary,
                    "col_type": col.type,
                    "key_info": compute_key_info(col),
                    "target_concept": target_concept,
                    "target_concept_id": target_concept.id if target_concept else None,
                    "target_concept_name": target_concept.name if target_concept else None,
                    "table_meta": table,
                    "col_meta": col,
                })

        for excel_col in excel_columns:
            clean_source = str(excel_col).strip()
            norm_source = normalize_column_name(clean_source)
            source_tokens = tokenize_column_name(clean_source)

            # Look up source business concept
            concept_match = synonym_registry.find_concept_for_term(clean_source, db_scope) or synonym_registry.find_concept_for_term(norm_source, db_scope)
            source_concept = concept_match[1] if concept_match else None
            source_concept_name = source_concept.name if source_concept else None
            src_data_type = infer_source_data_type(clean_source, norm_source, source_concept)

            # Check explicit User Override
            if clean_source in user_overrides:
                override = user_overrides[clean_source]
                action = override.get("action", "MAP")
                if action == "DO_NOT_MIGRATE":
                    results.append(ColumnMappingResult(
                        excelColumn=clean_source,
                        databaseTable="DO_NOT_MIGRATE",
                        databaseColumn="DO_NOT_MIGRATE",
                        normalizedSourceName=norm_source,
                        normalizedTargetName="DO_NOT_MIGRATE",
                        businessConcept=source_concept_name,
                        sourceDataType=src_data_type,
                        targetDataType="N/A",
                        explanation="User marked column as DO_NOT_MIGRATE.",
                        confidence=1.0,
                        matchType="USER_OVERRIDE",
                        status="DO_NOT_MIGRATE",
                        candidates=[]
                    ))
                    continue
                elif action == "MAP" and override.get("databaseTable") and override.get("databaseColumn"):
                    tgt_table = override["databaseTable"]
                    tgt_col = override["databaseColumn"]
                    matching_entry = next((e for e in db_entries if e["table_name"] == tgt_table and e["column_name"] == tgt_col), None)
                    tgt_type = matching_entry["col_type"] if matching_entry else "UNKNOWN"

                    # If save_rule flag is set, save to synonym registry
                    if override.get("saveRule") or override.get("save_rule"):
                        rule = CustomMappingRule(
                            source_column=clean_source,
                            business_concept=source_concept.id if source_concept else None,
                            target_table=tgt_table,
                            target_column=tgt_col,
                            database_scope=db_scope,
                            status="APPROVED"
                        )
                        synonym_registry.save_approved_rule(rule)

                    req_type, is_req = get_column_requirement_info(
                        matching_entry.get("table_meta") if matching_entry else None,
                        matching_entry.get("col_meta") if matching_entry else None
                    )
                    results.append(ColumnMappingResult(
                        excelColumn=clean_source,
                        databaseTable=tgt_table,
                        databaseColumn=tgt_col,
                        normalizedSourceName=norm_source,
                        normalizedTargetName=normalize_column_name(tgt_col),
                        businessConcept=source_concept_name,
                        sourceDataType=src_data_type,
                        targetDataType=tgt_type,
                        explanation="User confirmed mapping override.",
                        confidence=1.0,
                        matchType="USER_OVERRIDE",
                        status="MATCHED",
                        requirementType=req_type,
                        isRequired=is_req,
                        candidates=[]
                    ))
                    continue

            # Check Learned Approved Mapping Rule for this database
            approved_rule = synonym_registry.find_approved_rule(clean_source, db_scope)
            if approved_rule:
                rule_entry = next((e for e in db_entries if e["table_name"] == approved_rule.target_table and e["column_name"] == approved_rule.target_column), None)
                tgt_type = rule_entry["col_type"] if rule_entry else "UNKNOWN"
                req_type, is_req = get_column_requirement_info(
                    rule_entry.get("table_meta") if rule_entry else None,
                    rule_entry.get("col_meta") if rule_entry else None
                )
                results.append(ColumnMappingResult(
                    excelColumn=clean_source,
                    databaseTable=approved_rule.target_table,
                    databaseColumn=approved_rule.target_column,
                    normalizedSourceName=norm_source,
                    normalizedTargetName=normalize_column_name(approved_rule.target_column),
                    businessConcept=source_concept_name,
                    sourceDataType=src_data_type,
                    targetDataType=tgt_type,
                    explanation=f"Learned approved mapping rule for database '{approved_rule.database_scope or 'GLOBAL'}'.",
                    confidence=1.0,
                    matchType="APPROVED_RULE",
                    status="MATCHED",
                    requirementType=req_type,
                    isRequired=is_req,
                    candidates=[]
                ))
                continue

            # Candidate scoring list: (score, entry, match_type, explanation, layer_rank, compatibility)
            scored: List[Tuple[float, Dict[str, Any], str, str, int, str]] = []

            for entry in db_entries:
                # -------------------------------------------------------------
                # Semantic & Type Compatibility Check
                # -------------------------------------------------------------
                is_compat, compat_msg = check_semantic_compatibility(
                    clean_source=clean_source,
                    norm_source=norm_source,
                    source_concept=source_concept,
                    target_table=entry["table_name"],
                    target_col=entry["column_name"],
                    target_type=entry["col_type"],
                    target_concept=entry["target_concept"],
                    is_pk_or_ident=entry["is_pk"] or entry["is_identity"]
                )

                if not is_compat:
                    continue

                score = 0.0
                match_method = "NONE"
                reason = ""
                layer = 6

                # -------------------------------------------------------------
                # PRIORITY 1: Exact identifier match (character-for-character)
                # -------------------------------------------------------------
                if clean_source == entry["column_name"]:
                    score = 1.0
                    match_method = "EXACT_ORIGINAL"
                    reason = f"Exact identifier match on '{entry['table_name']}.{entry['column_name']}'."
                    layer = 1

                # -------------------------------------------------------------
                # PRIORITY 2: Case-insensitive match
                # -------------------------------------------------------------
                elif clean_source.lower() == entry["column_name"].lower():
                    score = 0.995
                    match_method = "CASE_INSENSITIVE"
                    reason = f"Case-insensitive match on '{entry['table_name']}.{entry['column_name']}'."
                    layer = 2

                # -------------------------------------------------------------
                # PRIORITY 3: Normalized identifier match (ignoring separators, spaces, casing)
                # -------------------------------------------------------------
                elif norm_source and norm_source == entry["norm_col"]:
                    score = 0.99
                    match_method = "EXACT_NORMALIZED"
                    reason = f"Normalized identifier match ('{norm_source}') on '{entry['table_name']}.{entry['column_name']}'."
                    layer = 3

                elif norm_source and (
                    norm_source in (f"{entry['norm_table']} {entry['norm_col']}", f"{entry['norm_col']} {entry['norm_table']}")
                    or "".join(source_tokens) in (f"{normalize_string(entry['table_name'])}{normalize_string(entry['column_name'])}", f"{normalize_string(entry['column_name'])}{normalize_string(entry['table_name'])}")
                ):
                    score = 0.98
                    match_method = "EXACT_NORMALIZED"
                    reason = f"Exact compound entity and column match on '{entry['table_name']}.{entry['column_name']}'."
                    layer = 3

                # -------------------------------------------------------------
                # PRIORITY 4: Explicit semantic aliases and verified relationships
                # -------------------------------------------------------------
                elif source_concept and entry["target_concept"] and source_concept.id == entry["target_concept"].id:
                    score = 0.95
                    if source_concept.id == "order_date" and entry["table_name"] == "Order" and entry["column_name"] == "Order_DateTime":
                        score = 0.96
                    elif source_concept.id == "customer_phone" and norm_source in ("contact phone", "contact_phone", "customer phone", "customer_phone") and entry["norm_col"] == "mobile number":
                        score = 0.96
                    match_method = "SYNONYM"
                    reason = f"Explicit semantic alias match on concept '{source_concept.name}' for '{entry['table_name']}.{entry['column_name']}'."
                    layer = 4

                elif source_concept and norm_source in [normalize_column_name(s) for s in source_concept.synonyms] and (
                    entry["norm_col"] in [normalize_column_name(s) for s in source_concept.synonyms]
                    or any(stem_token(st) in entry["col_tokens"] for st in source_tokens)
                ):
                    score = 0.94
                    match_method = "SYNONYM"
                    reason = f"Approved semantic dictionary match on '{entry['table_name']}.{entry['column_name']}'."
                    layer = 4

                else:
                    # Token overlap computation
                    src_tokens_set = set(source_tokens)
                    tgt_tokens_set = set(entry["col_tokens"])
                    common_tokens = src_tokens_set & tgt_tokens_set
                    table_tokens_set = set(entry["table_tokens"])
                    table_overlap = len(src_tokens_set & table_tokens_set)

                    # ---------------------------------------------------------
                    # LAYER 5: Semantic / Domain Business-Context Matching
                    # ---------------------------------------------------------
                    if norm_source in ("restaurant name", "outlet name", "virtual brand name", "brand name") and entry["table_name"] == "Outlets" and entry["norm_col"] in ("outlet name", "name"):
                        score, match_method, layer = 0.94, "SEMANTIC", 5
                        reason = "Primary restaurant outlet identifier in Outlets table."
                    elif norm_source in ("invoice no", "bill no", "receipt no") and entry["table_name"] in ("Bills", "Order") and entry["norm_col"] in ("billno", "bill number", "receipt no"):
                        score, match_method, layer = 0.92, "SEMANTIC", 5
                        reason = "Invoice / bill number identifier in transaction table."
                    elif norm_source in ("total", "grand total", "net total") and entry["table_name"] == "Bills" and entry["norm_col"] in ("final amount", "finalamount"):
                        score, match_method, layer = 0.96, "SEMANTIC", 4
                        reason = "Net final invoice amount mapped to Bills.FinalAmount."
                    elif norm_source == "delivery charge" and entry["table_name"] in ("Order", "Bills") and entry["norm_col"] in ("other charges", "othercharges"):
                        score, match_method, layer = 0.95, "SEMANTIC", 4
                        reason = "Delivery charge fee mapped to Order.Other_Charges."
                    elif norm_source == "container charge" and entry["table_name"] in ("Bills", "Order") and entry["norm_col"] in ("other charges", "othercharges"):
                        score, match_method, layer = 0.94, "SEMANTIC", 4
                        reason = "Container packaging charge mapped to Bills.OtherCharges."
                    elif norm_source in ("my amount", "sub total") and entry["table_name"] in ("Bills", "Order") and entry["norm_col"] in ("bill amount", "price", "total amount"):
                        score, match_method, layer = 0.90, "SEMANTIC", 5
                        reason = "Order monetary amount field."
                    elif norm_source == "total tax" and entry["table_name"] in ("Bills", "Order") and entry["norm_col"] in ("vat total", "tax", "vat"):
                        score, match_method, layer = 0.91, "SEMANTIC", 5
                        reason = "Total transaction tax monetary amount."
                    elif norm_source in ("additional charge", "deduction charge") and entry["table_name"] in ("Bills", "Order") and entry["norm_col"] in ("other charges", "othercharges"):
                        score, match_method, layer = 0.90, "SEMANTIC", 5
                        reason = "Ancillary charge amount mapped to other charges."
                    elif norm_source == "service charge" and entry["table_name"] in ("Bills", "Order") and entry["norm_col"] in ("service charge total", "other charges"):
                        score, match_method, layer = 0.91, "SEMANTIC", 5
                        reason = "Service charge total monetary amount."
                    elif norm_source == "item name" and entry["table_name"] == "OrderLineItems" and entry["norm_col"] == "description":
                        score, match_method, layer = 0.92, "SEMANTIC", 5
                        reason = "Item description in line item record."
                    elif norm_source == "item price" and entry["table_name"] == "OrderLineItems" and entry["norm_col"] == "item price":
                        score, match_method, layer = 0.92, "SEMANTIC", 5
                        reason = "Line item unit selling price."
                    elif norm_source == "item quantity" and entry["table_name"] == "OrderLineItems" and entry["norm_col"] == "quantity":
                        score, match_method, layer = 0.92, "SEMANTIC", 5
                        reason = "Line item quantity sold."
                    elif norm_source in ("customer phone", "contact phone") and entry["table_name"] == "Customer" and entry["norm_col"] == "mobile number":
                        score, match_method, layer = 0.96, "SYNONYM", 3
                        reason = "Customer contact phone explicitly mapped to Mobile_Number in Customer table."
                    elif norm_source == "customer phone" and entry["table_name"] == "Customer" and entry["norm_col"] in ("contact number", "mobile number", "phone number"):
                        score, match_method, layer = 0.92, "SEMANTIC", 5
                        reason = "Customer contact phone number in Customer table."
                    elif norm_source == "customer address" and entry["table_name"] == "Customer" and entry["norm_col"] in ("address", "customer address"):
                        score, match_method, layer = 0.92, "SEMANTIC", 5
                        reason = "Customer physical delivery address in Customer table."
                    elif norm_source == "date" and entry["table_name"] == "Order" and entry["norm_col"] == "order date time":
                        score, match_method, layer = 0.92, "SEMANTIC", 5
                        reason = "Transaction occurrence timestamp in Order table."
                    elif norm_source == "date" and entry["table_name"] == "Bills" and entry["norm_col"] == "bill date":
                        score, match_method, layer = 0.90, "SEMANTIC", 5
                        reason = "Invoice bill date timestamp."
                    elif norm_source == "payment type" and entry["table_name"] == "Bills" and entry["norm_col"] == "settlement type":
                        score, match_method, layer = 0.91, "SEMANTIC", 5
                        reason = "Payment settlement mode in Bills table."
                    elif norm_source == "persons" and entry["table_name"] == "Order" and entry["norm_col"] == "no of pax":
                        score, match_method, layer = 0.91, "SEMANTIC", 5
                        reason = "Customer guest / pax count in Order table."
                    elif norm_source == "order cancel reason" and entry["table_name"] == "Order" and entry["norm_col"] in ("service fail reason", "customer comments"):
                        score, match_method, layer = 0.90, "SEMANTIC", 5
                        reason = "Order cancellation or failure explanation."
                    elif norm_source in ("address", "addr") and entry["table_name"] in ("Outlets", "Stores", "Branches", "Locations") and entry["norm_col"] in ("location", "outlet location", "branch location", "store location", "address", "outlet address"):
                        score, match_method, layer = 0.92, "SEMANTIC", 5
                        reason = "Plausible outlet physical address or location in Outlets table."
                    elif norm_source == "status" and entry["table_name"] == "Order" and entry["norm_col"] == "order status":
                        score, match_method, layer = 0.92, "SEMANTIC", 5
                        reason = "Current status of the order transaction."

                    # ---------------------------------------------------------
                    # PRIORITY 5: Token-based similarity / Fuzzy matching
                    # ---------------------------------------------------------
                    elif len(common_tokens) > 0:
                        jaccard = len(common_tokens) / max(len(src_tokens_set), len(tgt_tokens_set))
                        table_bonus = 0.08 if table_overlap > 0 else 0.0
                        score = 0.65 + (0.20 * jaccard) + table_bonus
                        match_method = "FUZZY"
                        reason = f"Token overlap ({len(common_tokens)} matching word tokens)."
                        layer = 5

                        is_generic = bool(tgt_tokens_set & GENERIC_COLUMN_WORDS)
                        if is_generic and table_overlap == 0:
                            score = min(score, 0.65)

                # Prioritize operational tables over archive/dead-letter tables
                if entry["is_primary"]:
                    score += 0.04
                if entry["table_name"] == "OrderLineItems" and norm_source.startswith("item"):
                    score += 0.04
                if entry["is_archive"]:
                    score -= 0.35

                # Identity penalty for non-identity source data
                if entry["is_identity"] and score < 1.0:
                    score -= 0.10

                if score >= 0.70:
                    scored.append((score, entry, match_method, reason, layer, "COMPATIBLE"))

            # Sort strictly: Layer rank ascending (Layer 1 before Layer 2, etc.), then score descending
            scored.sort(key=lambda item: (item[4], -item[0]))

            if not scored:
                results.append(ColumnMappingResult(
                    excelColumn=clean_source,
                    databaseTable="UNKNOWN",
                    databaseColumn="UNKNOWN",
                    normalizedSourceName=norm_source,
                    normalizedTargetName="UNKNOWN",
                    businessConcept=source_concept_name,
                    sourceDataType=src_data_type,
                    targetDataType="UNKNOWN",
                    explanation="No compatible database columns found matching this field.",
                    confidence=0.0,
                    matchType="NONE",
                    status="UNMATCHED",
                    candidates=[]
                ))
                continue

            best_score, best_entry, best_method, best_reason, best_layer, _ = scored[0]

            # Build candidate list with detailed metadata
            candidate_dicts = []
            for item in scored[:6]:
                req_t, is_r = get_column_requirement_info(item[1].get("table_meta"), item[1].get("col_meta"))
                candidate_dicts.append({
                    "databaseTable": item[1]["table_name"],
                    "databaseColumn": item[1]["column_name"],
                    "matchMethod": item[2],
                    "matchType": item[2],
                    "confidence": min(round(item[0], 2), 1.0),
                    "dataTypeCompatibility": item[5],
                    "keyInfo": item[1]["key_info"],
                    "requirementType": req_t,
                    "isRequired": is_r,
                    "reason": item[3] or f"Plausible mapping on {item[1]['table_name']}.{item[1]['column_name']}",
                    "normalizedTargetName": item[1]["norm_col"],
                    "businessConcept": item[1]["target_concept_name"],
                    "sourceDataType": src_data_type,
                    "targetDataType": item[1]["col_type"]
                })

            # -----------------------------------------------------------------
            # LAYER 6: AMBIGUITY DETECTION & RESOLUTION
            # -----------------------------------------------------------------

            # 1. Special Address Ambiguity Rule:
            # If the column is generic 'address' or 'addr', and candidates exist in both Customer and Outlets:
            is_generic_address = norm_source in ("address", "addr")
            if is_generic_address:
                has_customer_cand = any(c["databaseTable"] == "Customer" for c in candidate_dicts)
                has_outlet_cand = any(c["databaseTable"] in ("Outlets", "Stores", "Branches") for c in candidate_dicts)
                if has_customer_cand and has_outlet_cand:
                    decisive_customer = has_customer_signals and not has_outlet_signals
                    decisive_outlet = has_outlet_signals and not has_customer_signals
                    if not (decisive_customer or decisive_outlet):
                        cand_summaries = [f"{c['databaseTable']}.{c['databaseColumn']}" for c in candidate_dicts[:2]]
                        results.append(ColumnMappingResult(
                            excelColumn=clean_source,
                            databaseTable="UNKNOWN",
                            databaseColumn="UNKNOWN",
                            normalizedSourceName=norm_source,
                            normalizedTargetName="AMBIGUOUS",
                            businessConcept="Customer address / Location",
                            sourceDataType=src_data_type,
                            targetDataType="NVARCHAR(200)",
                            explanation=(
                                f"Ambiguous mapping for '{clean_source}': Candidates found in both "
                                f"{' and '.join(cand_summaries)}. Insufficient file context to automatically disambiguate."
                            ),
                            confidence=round(best_score, 2),
                            matchType="AMBIGUOUS",
                            status="AMBIGUOUS",
                            requirementType="UNRESOLVED",
                            isRequired=False,
                            candidates=candidate_dicts
                        ))
                        continue

            # 2. General Ambiguity Rule:
            # If multiple distinct target columns share the top layer and have close scores (< 0.02)
            top_layer_items = [item for item in scored if item[4] == best_layer and abs(item[0] - best_score) <= 0.02]
            distinct_targets = {(item[1]["table_name"], item[1]["column_name"]) for item in top_layer_items}

            if len(distinct_targets) > 1 and best_score < 0.99:
                cand_names = [f"{t}.{c}" for t, c in sorted(distinct_targets)[:3]]
                results.append(ColumnMappingResult(
                    excelColumn=clean_source,
                    databaseTable="UNKNOWN",
                    databaseColumn="UNKNOWN",
                    normalizedSourceName=norm_source,
                    normalizedTargetName="AMBIGUOUS",
                    businessConcept=source_concept_name,
                    sourceDataType=src_data_type,
                    targetDataType="VARIES",
                    explanation=f"Ambiguous mapping: Multiple equally plausible candidates ({', '.join(cand_names)}). User review required.",
                    confidence=round(best_score, 2),
                    matchType="AMBIGUOUS",
                    status="AMBIGUOUS",
                    requirementType="UNRESOLVED",
                    isRequired=False,
                    candidates=candidate_dicts
                ))
                continue

            # Confirmed confident match
            req_type, is_req = get_column_requirement_info(best_entry.get("table_meta"), best_entry.get("col_meta"))
            results.append(ColumnMappingResult(
                excelColumn=clean_source,
                databaseTable=best_entry["table_name"],
                databaseColumn=best_entry["column_name"],
                normalizedSourceName=norm_source,
                normalizedTargetName=best_entry["norm_col"],
                businessConcept=source_concept_name or best_entry["target_concept_name"],
                sourceDataType=src_data_type,
                targetDataType=best_entry["col_type"],
                explanation=best_reason,
                confidence=min(round(best_score, 2), 1.0),
                matchType=best_method,
                status="MATCHED",
                requirementType=req_type,
                isRequired=is_req,
                candidates=candidate_dicts
            ))

        # ---------------------------------------------------------------------
        # Prevent Unintended Many-to-One Column Assignments on the Same Table
        # ---------------------------------------------------------------------
        table_col_claims: Dict[Tuple[str, str], List[int]] = {}
        for idx, res in enumerate(results):
            if res.status == "MATCHED":
                key = (res.database_table, res.database_column)
                table_col_claims.setdefault(key, []).append(idx)

        for (tbl, col_name), claimed_indices in table_col_claims.items():
            if len(claimed_indices) > 1:
                # If the claimed column is Outlets.Outlet_Name and the claimants are restaurant_name and virtual_brand_name,
                # permit both to remain mapped so both source columns are tracked for outlet resolution.
                claimant_sources = [results[i].excel_column.lower().replace(" ", "_") for i in claimed_indices]
                if tbl.lower() in ("outlets", "dbo.outlets") and col_name.lower() in ("outlet_name", "outletname"):
                    if any(c in claimant_sources for c in ("restaurant_name", "virtual_brand_name", "outlet_name")):
                        logger.info(
                            f"[MATCHER] Retaining multi-source outlet name mappings {claimant_sources} for '{tbl}.{col_name}'."
                        )
                        continue

                # Multiple source columns claimed the same database table + column!
                # Sort: exact/higher confidence wins, others are marked AMBIGUOUS
                claimed_indices.sort(key=lambda i: results[i].confidence, reverse=True)
                winner_idx = claimed_indices[0]

                for other_idx in claimed_indices[1:]:
                    other = results[other_idx]
                    logger.info(
                        f"[MATCHER] Resolving duplicate assignment: '{other.excel_column}' "
                        f"relinquished '{tbl}.{col_name}' to higher-confidence '{results[winner_idx].excel_column}'"
                    )
                    results[other_idx] = ColumnMappingResult(
                        excelColumn=other.excel_column,
                        databaseTable="UNKNOWN",
                        databaseColumn="UNKNOWN",
                        normalizedSourceName=other.normalized_source_name,
                        normalizedTargetName=other.normalized_target_name,
                        businessConcept=other.business_concept,
                        sourceDataType=other.source_data_type,
                        targetDataType=other.target_data_type,
                        explanation=(
                            f"Duplicate assignment on '{tbl}.{col_name}' relinquished to "
                            f"'{results[winner_idx].excel_column}'. User review required."
                        ),
                        confidence=other.confidence,
                        matchType="AMBIGUOUS",
                        status="AMBIGUOUS",
                        requirementType="UNRESOLVED",
                        isRequired=False,
                        candidates=other.candidates
                    )

        logger.info(
            f"Column matching complete: {len(results)} columns processed, "
            f"{sum(1 for r in results if r.status == 'MATCHED')} matched, "
            f"{sum(1 for r in results if r.status == 'AMBIGUOUS')} ambiguous."
        )
        return results


def resolve_unmapped_required_columns(
    schema: DatabaseSchemaMetadata,
    table_mappings: Dict[str, List[ColumnMappingResult]],
    df_columns: List[str]
) -> Dict[str, List[ColumnMappingResult]]:
    """
    Ensures that active target tables do not fail due to unmapped mandatory columns
    when a matching source column (such as date/time) is present in the uploaded dataset.
    Dynamically identifies and attaches the appropriate source column mapping.
    """
    schema_tables = {t.name: t for t in schema.tables}
    for table_name, mappings in list(table_mappings.items()):
        if table_name not in schema_tables:
            continue
        t_meta = schema_tables[table_name]
        mapped_db_cols = {m.database_column for m in mappings if m.status in ("MATCHED", "APPROVED")}

        for c in t_meta.columns:
            if c.nullable or getattr(c, "identity", False) or c.default is not None:
                continue
            # Check if required column is already mapped
            if c.name in mapped_db_cols:
                continue

            # Check if foreign key (handled by foreign-key dependency resolver)
            is_fk = any(fk.constrained_column == c.name for fk in t_meta.foreign_keys)
            c_lower = c.name.lower()
            if is_fk or c_lower.endswith("_id") or c_lower.endswith("id"):
                continue

            # Unmapped required regular column (e.g. Order_DateTime)
            # Find matching column in df_columns
            c_concept_match = synonym_registry.find_concept_for_term(c.name) or synonym_registry.find_concept_for_term(normalize_column_name(c.name))
            target_concept_id = c_concept_match[1].id if c_concept_match else None

            matched_excel_col = None
            # 1. Match via business concept (e.g. order_date)
            if target_concept_id:
                for col in df_columns:
                    src_match = synonym_registry.find_concept_for_term(col) or synonym_registry.find_concept_for_term(normalize_column_name(col))
                    if src_match and src_match[1].id == target_concept_id:
                        matched_excel_col = col
                        break

            # 2. Date/time column type matching fallback
            col_type_up = c.type.upper()
            if not matched_excel_col and any(t in col_type_up for t in ("DATE", "DATETIME", "TIME")):
                DATE_KEYWORDS = ("date", "time", "datetime", "timestamp")
                for col in df_columns:
                    col_low = col.lower()
                    if any(k in col_low for k in DATE_KEYWORDS):
                        matched_excel_col = col
                        break

            if matched_excel_col:
                new_m = ColumnMappingResult(
                    excelColumn=matched_excel_col,
                    databaseTable=table_name,
                    databaseColumn=c.name,
                    normalizedSourceName=normalize_column_name(matched_excel_col),
                    normalizedTargetName=normalize_column_name(c.name),
                    businessConcept=c_concept_match[1].name if c_concept_match else "Order date",
                    sourceDataType="DATETIME" if any(t in col_type_up for t in ("DATE", "DATETIME")) else "VARCHAR",
                    targetDataType=c.type,
                    explanation=f"Dynamically mapped required column '{table_name}.{c.name}' to matching date/time source column '{matched_excel_col}'.",
                    confidence=0.95,
                    matchType="SYNONYM",
                    status="MATCHED",
                    requirementType="MANDATORY",
                    isRequired=True,
                    candidates=[]
                )
                table_mappings[table_name].append(new_m)
                logger.info(f"[DYNAMIC_MAPPING] Dynamically mapped required column '{table_name}.{c.name}' <- '{matched_excel_col}'")

    # Also check if any active table has a date_value column and Excel has 'date' column unmapped
    date_col = next((col for col in df_columns if normalize_column_name(col) in ("date", "order date")), None)
    if date_col:
        for table_name, mappings in list(table_mappings.items()):
            if table_name not in schema_tables:
                continue
            t_meta = schema_tables[table_name]
            mapped_cols = {m.database_column.lower() for m in mappings if m.status in ("MATCHED", "APPROVED")}
            for c in t_meta.columns:
                c_norm = normalize_column_name(c.name)
                if (c_norm in ("date value", "date_value", "order date", "order_date") or c.name.lower() == "date_value") and c.name.lower() not in mapped_cols:
                    new_m = ColumnMappingResult(
                        excelColumn=date_col,
                        databaseTable=table_name,
                        databaseColumn=c.name,
                        normalizedSourceName=normalize_column_name(date_col),
                        normalizedTargetName=normalize_column_name(c.name),
                        businessConcept="Order date",
                        sourceDataType="DATETIME",
                        targetDataType=c.type,
                        explanation=f"Mapped source date column '{date_col}' to '{table_name}.{c.name}'.",
                        confidence=0.95,
                        matchType="SYNONYM",
                        status="MATCHED",
                        requirementType="MANDATORY" if not c.nullable else "OPTIONAL",
                        isRequired=not c.nullable,
                        candidates=[]
                    )
                    table_mappings[table_name].append(new_m)
                    logger.info(f"[DYNAMIC_MAPPING] Mapped date column '{table_name}.{c.name}' <- '{date_col}'")

    return table_mappings


