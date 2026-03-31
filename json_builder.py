import json
import base64
import gzip
from datetime import datetime, timedelta
import uuid
import re
from typing import Dict, Any, Optional

class IngestionPayloadBuilder:
    REQUIRED_FIELDS = []
    VALID_ENTITY_TYPES=[]
    VALID_PRIORITIES=[]
    VALID_DOCUMENT_TYPES= {

    }

    @staticmethod
    def generate_event_id() -> str:
        return f"EVT_{datetime.now().strftime('%Y%m%d')}_{uuid.uuid4().hex[:8].upper()}"

    @staticmethod
    def encode_file_binary(file_data:bytes, compress: bool = True) -> str:
        if compress:
            compressed= gzip.compress(file_data)
            return base64.b64encode(compressed).decode('utf-8')
        return base64.b64encode(file_data).decode('utf-8')
    
    @classmethod
    def extract_member_id(cls, email_data: Dict[str, Any]) -> Optional[str]:
        text_sources = [
            email_data.get('subject', ''),
            email_data.get('body_preview', ''),
            email_data.get('sender', '')
        ]

        combined_text = ' '.join(text_sources).lower()

        patterns= []

        for pattern in patterns:
            match = re.search(pattern, combined_text)
            if match:
                member_id = match.group(1)
                if 8 <= len(member_id) <= 12:
                    return member_id
        
        return None
    
    @classmethod
    def classify_document_type(cls, filename: str, subject:str= '', body: str= '') -> str:
        content= f"{filename} {subject} {body}".lower()

        classifications = {
        'PROOFINC': ['proof', 'income', 'salary', 'payslip', 'earning'],
        'CLAIM': ['claim', 'medical', 'treatment', 'doctor', 'hospital'],
        'INVOICE': ['invoice', 'bill', 'account', 'statement', 'payment'],
        'IDENTITY': ['id', 'identity', 'passport', 'license', 'copy'],
        'MEDICAL': ['report', 'results', 'test', 'scan', 'xray', 'lab'],
        'PREAUTH': ['authorization', 'preauth', 'approval', 'permission']
        }   
        
        for doc_type, keywords in classifications.items():
            if any(keyword in content for keyword in keywords):
                return doc_type
        
        return ''
    
    @classmethod
    def build_payload(cls, email_data: Dict[str, Any], attachment_data: bytes, filename: str, event_id: str=None, **kwargs) -> Dict[str, Any]:
        if not event_id:
            event_id = cls.generate_event_id()
        
        member_id= kwargs.get('member') or cls.extract_member_id(email_data)
        ##Classify document type
        document_type= kwargs.get('document_type') or cls.classify_document_type(filename, email_data.get('subject', ''), email_data.get('body_preview', ''))

        ##Parse email date
        email_date= email_data.get('email_date')
        if isinstance(email_date, str):
            try:
                email_date= datetime.fromisoformat(email_date.replace('Z', '+00:00'))
            except:
                email_date= datetime.now()
        elif not email_date:
            email_date= datetime.now()

        payload = {
            "member": member_id or "UNKNOWN",
            "schemeCode": kwargs.get('scheme_code', 50),  # Bestmed scheme code
            "correspondenceDate": email_date.isoformat(),
            "expiryDate": kwargs.get('expiry_date') or (datetime.now() + timedelta(days=30)).isoformat(),
            "provider": kwargs.get('provider'),
            "barcode": kwargs.get('barcode'),
            "documentType": document_type,
            "documentSubType": kwargs.get('document_sub_type'),
            "payer": kwargs.get('payer'),
            "intermediary": kwargs.get('intermediary'),
            "source": "EMAIL",
            "encoding": "application/gzip" if kwargs.get('compress', True) else "application/octet-stream",
            "carbonCopy": kwargs.get('carbon_copy'),
            "physicalLocation": kwargs.get('physical_location'),
            "thirdParty": kwargs.get('third_party'),
            "dependantNo": kwargs.get('dependant_no'),
            "entityType": kwargs.get('entity_type', 'member'),
            "fileName": filename,
            "fileBinary": cls.encode_file_binary(attachment_data, kwargs.get('compress', True)),
            "externalReference": email_data.get('subject'),
            "cellPhoneNumber": kwargs.get('cell_phone_number'),
            "priority": kwargs.get('priority', 'N'),
            "note": f"Email from: {email_data.get('sender', 'Unknown')} | Subject: {email_data.get('subject', 'No Subject')}",
            "dueDate": kwargs.get('due_date'),
            "schemeAuthorisationReference": kwargs.get('scheme_authorisation_reference')
        }

        return payload
    
    @classmethod
    def validate_payload(cls, payload: Dict[str, Any]) -> tuple[bool, list[str]]:
        """
        Validate payload against documentindex.json schema requirements
        
        Returns:
            (is_valid, error_messages)
        """
        errors = []
        
        # Check required fields
        for field in cls.REQUIRED_FIELDS:
            if not payload.get(field) and field != 'file_binary':  # file_binary checked separately
                errors.append(f"Missing required field: {field}")
        
        # Validate specific field formats
        if payload.get('member') == 'UNKNOWN':
            errors.append("Member ID could not be extracted from email")
        
        # Validate entity type
        if payload.get('entityType') not in cls.VALID_ENTITY_TYPES:
            errors.append(f"Invalid entityType '{payload.get('entityType')}'. Must be one of: {cls.VALID_ENTITY_TYPES}")
        
        # Validate priority
        if payload.get('priority') not in cls.VALID_PRIORITIES:
            errors.append(f"Invalid priority '{payload.get('priority')}'. Must be one of: {cls.VALID_PRIORITIES}")
        
        # Validate document type
        if payload.get('documentType') not in cls.VALID_DOCUMENT_TYPES:
            errors.append(f"Invalid documentType '{payload.get('documentType')}'. Must be one of: {list(cls.VALID_DOCUMENT_TYPES.keys())}")
        
        # Validate dates
        for date_field in ['correspondenceDate', 'expiryDate', 'dueDate']:
            if payload.get(date_field):
                try:
                    datetime.fromisoformat(payload[date_field].replace('Z', '+00:00'))
                except ValueError:
                    errors.append(f"Invalid {date_field} format. Must be ISO 8601 format")
        
        # Validate scheme code
        scheme_code = payload.get('schemeCode')
        if scheme_code and not isinstance(scheme_code, int):
            errors.append("schemeCode must be an integer")
        
        # Check file binary exists and is valid base64
        file_binary = payload.get('fileBinary')
        if not file_binary:
            errors.append("Missing fileBinary data")
        else:
            try:
                base64.b64decode(file_binary)
            except Exception:
                errors.append("Invalid fileBinary - must be valid base64")
        
        return len(errors) == 0, errors
    
    @classmethod
    def save_payload_to_db(cls, supabase_client, email_id: str, event_id: str, payload: Dict[str, Any]) -> bool:
        """Save validated payload to ingestion_payloads table"""
        try:
            # Convert payload to database format
            db_payload = {
                'email_id': email_id,
                'event_id': event_id,
                'member': payload.get('member'),
                'scheme_code': payload.get('schemeCode'),
                'correspondence_date': payload.get('correspondenceDate'),
                'expiry_date': payload.get('expiryDate'),
                'provider': payload.get('provider'),
                'barcode': payload.get('barcode'),
                'document_type': payload.get('documentType'),
                'document_sub_type': payload.get('documentSubType'),
                'payer': payload.get('payer'),
                'intermediary': payload.get('intermediary'),
                'source': payload.get('source'),
                'encoding': payload.get('encoding'),
                'carbon_copy': payload.get('carbonCopy'),
                'physical_location': payload.get('physicalLocation'),
                'third_party': payload.get('thirdParty'),
                'dependant_no': payload.get('dependantNo'),
                'entity_type': payload.get('entityType'),
                'file_name': payload.get('fileName'),
                'file_binary': payload.get('fileBinary'),
                'external_reference': payload.get('externalReference'),
                'cell_phone_number': payload.get('cellPhoneNumber'),
                'priority': payload.get('priority'),
                'note': payload.get('note'),
                'due_date': payload.get('dueDate'),
                'scheme_authorisation_reference': payload.get('schemeAuthorisationReference'),
                'validation_status': 'valid'
            }
            
            result = supabase_client.table('ingestion_payloads').insert(db_payload).execute()
            return True
        except Exception as e:
            print(f"Error saving payload to database: {e}")
            return False
