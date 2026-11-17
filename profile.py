def normalize_leader_path(person: dict) -> str | None:
            # 1. Safely retrieve the leader path using your fallback chain
            raw_path = (
                person.get("LeaderPath[3]")
                or person.get("LeaderPath[2]")
                or person.get("LeaderPath[1]")
                or person.get("LeaderPath[0]")
            )

            if raw_path is None:
                return None
            value = raw_path
            # 2. Handle MongoDB $oid dictionary structure
            # Already a dict
            if isinstance(value, dict):
                value = value.get("$oid")
        
            if not isinstance(value, str):
                return None
        
            # Works for a plain id or a JSON-looking string containing one
            match = re.search(r"\b[0-9a-fA-F]{24}\b", value)
            return match.group(0) if match else None

import re

def extract_oid(value):
    """
    Extract a MongoDB ObjectId string from either:
      - a plain string:  "692d979750555e9dfdccbd56"
      - a dict:          {"$oid": "692d979750555e9dfdccbd56"}
      - a JSON string:   '{"$oid":"692d979750555e9dfdccbd56"}'
    Returns the 24-char hex string, or None if no valid id is found.
    """
    # Already a dict
    if isinstance(value, dict):
        value = value.get("$oid")

    if not isinstance(value, str):
        return None

    # Works for a plain id or a JSON-looking string containing one
    match = re.search(r"\b[0-9a-fA-F]{24}\b", value)
    return match.group(0) if match else None


person =  {'_id': '69eb9a9d4855fb59a30d5267', 'Name': 'Aaron', 'Surname': 'Muya', 'Number': '07334169179', 'Email': 'muya10@gmail.com', 'Address': '72 Malborough Road', 'Gender': 'Male', 'Birthday': '2010/05/12', 'Date Created': '2026-04-24T14:30:21.397+00:00', 'InvitedBy': 'Shane van der Walt', 'Stage': 'Win', 'UpdatedAt': '2026-04-24T14:30:21.397+00:00', 'LeaderId': '{"$oid":"692d97e850555e9dfdccebd1"}', 'LeaderPath[0]': '{"$oid":"692d97e550555e9dfdccea4e"}', 'LeaderPath[1]': '692d97e850555e9dfdccebd1', 'LeaderPath[2]': None, 'LeaderPath[3]': None, 'Organization': 'Active Church', 'DecisionDate': None, 'DecisionType': None, 'LastDecisionDate': None, 'TotalRecommitments': None, 'Leader @1': 'Gavin Enslin', 'Leader @12': 'Shane van der Walt', 'Leader @144': None, 'Leader @1728': None, 'FirstDecisionDate': None, 'user_id': None, 'SeniorPastor': None, 'DateCreated': '2026-04-24T14:30:21.397Z', 'imported_by': None, 'org_id': '69c63afc4c3e2fdfc5a4840d', 'person_id': 'c4a231c0-271e-4044-8fb0-46f85dbc9681'}
print(normalize_leader_path(person))  # Output: 692d97e850555e9dfdccebd1