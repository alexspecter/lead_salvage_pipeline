# Field Names (canonical internal names)
FIELD_Email = "email"
FIELD_Phone = "phone"
FIELD_FirstName = "first_name"
FIELD_LastName = "last_name"
FIELD_Company = "company"
FIELD_JobTitle = "job_title"
FIELD_Date = "date"

REQUIRED_FIELDS = {FIELD_Email, FIELD_Phone}  # At least one of these

# Log phases
PHASE_SETUP = "SETUP"
PHASE_1_DETERMINISTIC = "PHASE_1"
PHASE_2_SEMANTIC = "PHASE_2"
PHASE_3_MERGE = "PHASE_3"

# ============================================
# Field Type Detection (for dynamic field handling)
# ============================================
# Maps canonical field types to possible column name variations
# Used to auto-detect field types from any input schema
FIELD_TYPE_PATTERNS = {
    "email": ["email", "e-mail", "mail", "email_address", "e_mail", "emailaddress"],
    "phone": ["phone", "tel", "telephone", "mobile", "cell", "contact_number", "phone_number", "phonenumber"],
    "first_name": ["first_name", "firstname", "first", "fname", "given_name", "givenname"],
    "last_name": ["last_name", "lastname", "last", "lname", "surname", "family_name", "familyname"],
    "date": ["date", "join_date", "start_date", "created", "modified", "dob", "birthday", "hire_date", "end_date"],
    "company": ["company", "organization", "org", "employer", "firm", "business", "company_name"],
    "job_title": ["job_title", "title", "position", "role", "job", "designation", "jobtitle"],
}

# Fields that should use placeholder for missing values
# Other fields will preserve empty/null as-is
PLACEHOLDER_ELIGIBLE_FIELDS = {
    FIELD_Email, FIELD_Phone, FIELD_FirstName, FIELD_LastName, 
    FIELD_Company, FIELD_JobTitle
}

# Fields that are numeric and should NOT get placeholder replacement
NUMERIC_FIELDS = {"age", "salary", "score", "rating", "count", "amount", "price", "cost"}

# Acronyms that should be fully uppercased in titles
PROFESSIONAL_ACRONYMS = [
    "HR", "CEO", "CFO", "COO", "CTO", "CIO", "CMO", "CRO", "CPO", 
    "VP", "SVP", "EVP", "AVP", 
    "IT", "KA", "BD", "R&D", "QA", "QC", 
    "AI", "ML", "B2B", "B2C", "SaaS", 
    "UX", "UI", 
    "PR", "ROI", "KPI", "SEO", "SEM", 
    "PMP", "CPA", "MBA", "PHD", "MD", "RN", "LPN"
]


