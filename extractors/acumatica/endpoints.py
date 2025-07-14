"""Acumatica API endpoint definitions"""

ACUMATICA_ENDPOINTS = {
    "customers": {
        "endpoint": "Customer",
        "description": "Customer master data",
        "key_field": "CustomerID"
    },
    "items": {
        "endpoint": "StockItem",
        "description": "Inventory items/products", 
        "key_field": "InventoryID"
    },
    "vendors": {
        "endpoint": "Vendor",
        "description": "Vendor master data",
        "key_field": "VendorID"
    },
    "sales_orders": {
        "endpoint": "SalesOrder", 
        "description": "Sales orders",
        "key_field": "OrderNbr"
    },
    "invoices": {
        "endpoint": "SalesInvoice",
        "description": "Sales invoices", 
        "key_field": "ReferenceNbr"
    },
    "purchase_orders": {
        "endpoint": "PurchaseOrder",
        "description": "Purchase orders",
        "key_field": "OrderNbr"
    }
}

def get_endpoint_config(endpoint_name: str) -> dict:
    """Get configuration for specific endpoint"""
    return ACUMATICA_ENDPOINTS.get(endpoint_name, {})

def get_all_endpoints() -> list:
    """Get list of all available endpoints"""
    return list(ACUMATICA_ENDPOINTS.keys())