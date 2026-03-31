import os
import extract_msg
from supabase import create_client, Client
from io import BytesIO
from flask import Flask, render_template_string, Response, jsonify, request, send_file
import threading
import queue
import time
import requests
import re
import json
import pandas as pd
import email
import mimetypes
from datetime import datetime
from dotenv import load_dotenv
from email import policy
import base64
import gzip
import json
import urllib.parse
import zipfile

# Load environment variables from .env file
load_dotenv()

mimetypes.init()


# --- SUPABASE CONFIG ---
SUPABASE_URL = os.getenv('SUPABASE_URL', 'http://127.0.0.1:54321')
SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_KEY')

BUCKET_NAME = "bestmed"               # Single bucket
INPUT_FOLDER = "Emails"                # Starting folder inside bucket
OUTPUT_FOLDER = "email_attachments"   # New folder inside same bucket
OUTPUT_CSV_NAME = f"{OUTPUT_FOLDER}/email_data.csv"


# Validate required environment variables
if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise ValueError("Missing required environment variables: SUPABASE_URL, SUPABASE_ANON_KEY, and SUPABASE_SERVICE_KEY must be set in .env file")

# --- INIT SUPABASE CLIENTS ---
# supabase_read: Uses anon key - for reads and storage operations
# supabase_write: Uses service role key - for database writes only (bypasses RLS)
supabase_read: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
supabase_write: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
supabase_server: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

records = []
processing_started = False
processing_complete = False
n8n_processing_started = False
n8n_processing_complete = False

# --- LOG QUEUE ---
log_queue = queue.Queue()

# Filebinary class 
class BestmedPayloadProcessor:
    """Process JSON payloads by adding file binary data"""
    @staticmethod
    def generate_event_id():
        """Generate unique Event ID with format: EVT_YYYYMMDD_XXXXXXXX"""
        import uuid
        return f"EVT_{datetime.now().strftime('%Y%m%d')}_{uuid.uuid4().hex[:8].upper()}"
    
    @staticmethod
    def download_and_encode_file(file_url_or_path):
        """Download file from Supabase storage and convert to gzipped base64"""
        try:
            # Extract storage path from URL if needed
            if file_url_or_path.startswith('http'):
                # Extract path from URL: http://127.0.0.1:54321/storage/v1/object/public/bestmed/path/to/file
                url_parts = file_url_or_path.split('/storage/v1/object/public/bestmed/')
                if len(url_parts) > 1:
                    storage_path = url_parts[1]
                    # URL decode the path
                    storage_path = urllib.parse.unquote(storage_path)
                else:
                    storage_path = file_url_or_path
            else:
                storage_path = file_url_or_path
            
            log(f"[FILE] Downloading file from storage path: {storage_path}")
            
            # Download file from Supabase storage
            file_data = supabase_read.storage.from_(BUCKET_NAME).download(storage_path)
            
            if not file_data:
                log(f"[ERROR] File not found or empty: {storage_path}")
                return None
            
            # Compress with gzip
            compressed_data = gzip.compress(file_data)
            
            # Convert to base64
            base64_data = base64.b64encode(compressed_data).decode('utf-8')
            
            log(f"[FILE] Successfully converted file. Original: {len(file_data)} bytes, Compressed: {len(compressed_data)} bytes, Base64: {len(base64_data)} chars")
            return base64_data
            
        except Exception as e:
            log(f"[ERROR] Failed to download and encode file {file_url_or_path}: {e}")
            return None 

    
    @staticmethod
    def get_email_folder_path(email_file_name):
        # corrected email attachments path extraction
        email_base_name= email_file_name.replace('.msg', '').replace('.eml', '')
        
        # bucket path: email_attachments/root/EmailName
        folder_path = f"{OUTPUT_FOLDER}/root/{email_base_name}"
        return folder_path

    @staticmethod
    def get_attachment_path_for_email(email_file_name, attachment_filename):
        """Build attachment path based on email structure"""
        # Remove .msg extension from email name
        email_base_name = email_file_name.replace('.msg', '').replace('.eml', '')
        
        # Build path: email_attachments/root/EmailName/attachment.ext
        attachment_path = f"{OUTPUT_FOLDER}/root/{email_base_name}/{attachment_filename}"
        
        return attachment_path
    
    @classmethod
    def create_zip_from_email_attachments(cls, email_file_name):
        try:
            folder_path = cls.get_email_folder_path(email_file_name)
            log(f"[ZIP] Searching for attachments in folder: {folder_path}")

            try:
                files= supabase_read.storage.from_(BUCKET_NAME).list(path=folder_path)
            except Exception as e:
                log(f"[ERROR] Failed to list files in folder {folder_path}: {e}")
                return None, 0
        
            if not files:
                log(f"[ZIP] No files found in folder: {folder_path}")
                return None, 0
            
            target_extensions = ['.pdf', '.docx', '.doc', '.png', '.jpeg', '.jpg']
            filtered_files = [
                f for f in files
                if f.get('name') and any(f['name'].lower().endswith(ext) for ext in target_extensions)
            ]

            if not filtered_files:
                log(f"[ZIP] No target attachments found in folder: {folder_path}")
                return None, 0
            
            log(f"[ZIP] Found {len(filtered_files)} target attachments for zipping.")

            # Create in-memory ZIP
            zip_buffer = BytesIO()
            file_count = 0

            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for file_info in filtered_files:
                    file_name= file_info['name']
                    file_path= f"{folder_path}/{file_name}"

                    try:
                        log(f"[ZIP] Adding file: {file_name}")
                        file_data = supabase_read.storage.from_(BUCKET_NAME).download(file_path)

                        if file_data:
                            zip_file.writestr(file_name, file_data)
                            file_count += 1
                            log(f"[ZIP] Added file to ZIP: {file_name}  ({len(file_data)} bytes)")
                        
                        else:
                            log(f"[WARN ZIP] Empty file, skipping: {file_name}")
                        
                    except Exception as e:
                        log(f"[ERROR] Failed to add file {file_name} to ZIP: {e}")
                        continue

            if file_count == 0:
                log(f"[ZIP ERROR] No files were added to the ZIP archive.")
                return None, 0
                
            zip_data= zip_buffer.getvalue()
            log(f"[ZIP] Created ZIP archive with {file_count} files, total size: {len(zip_data)} bytes")

            return zip_data, file_count

        except Exception as e:    
            log(f"[ERROR] Failed to create ZIP from email attachments for {email_file_name}: {e}")
            return None, 0

    @classmethod
    def encode_zip_to_base64(cls, zip_data):
        try:
            compressed_data= gzip.compress(zip_data)
            base64_data= base64.b64encode(compressed_data).decode('utf-8')

            log(f"[ZIP ENCODING] Successfully encoded ZIP. Compressed: {len(compressed_data)} bytes, Base64: {len(base64_data)} chars")
            return base64_data
            
        except Exception as e:
            log(f"[ERROR] Failed to encode ZIP to base64: {e}")
            return None
    
    @classmethod
    def process_email_json_payload(cls, email_file_name):
        """
        Process an email's JSON payload by adding file binary data
        
        Args:
            email_file_name: Name of the email file (e.g., "email.msg" or "email.eml")
        """
        try:
            # Get email record with JSON payload
            result = supabase_read.table('emails').select(
                'id, file_name, json_payload, classification_check'
            ).eq('file_name', email_file_name).execute()
            
            if not result.data:
                log(f"[ERROR] Email not found: {email_file_name}")
                return False
            
            email_record = result.data[0]
            json_payload = email_record.get('json_payload')
            
            if not json_payload:
                log(f"[ERROR] No JSON payload found for email: {email_file_name}")
                # Update status to failed
                supabase_write.table('emails').update({
                    'payload_status': 'failed'
                }).eq('id', email_record['id']).execute()
                return False
            
            # Parse JSON payload if it's a string
            if isinstance(json_payload, str):
                json_payload = json.loads(json_payload)
            
            log(f"[PROCESS] Processing JSON payload for email: {email_file_name}")
            
            zip_data, file_count = cls.create_zip_from_email_attachments(email_file_name)
            
            if not zip_data or file_count == 0:
                log(f"[WARN] No attachments found for email: {email_file_name}, setting fileBinary and fileName to null")
    
                # ✅ Set file binary and filename to null when no attachments
                json_payload['fileBinary'] = None
                json_payload['fileName'] = None
                json_payload['encoding'] = None
    
                # Replace documentType with classification_check
                classification_check = email_record.get('classification_check')
                if classification_check:
                    # ✅ Convert to code before storing
                    document_type_code = cls.get_document_type_code(classification_check)
                    json_payload['documentType'] = document_type_code  # ✅ Use code
    
                # ✅ Format and save payload even without attachments
                formatted_payload = format_bestmed_payload(json_payload)
    
                update_result = supabase_write.table('emails').update({
                    'json_payload': json.dumps(formatted_payload),
                    'payload_status': 'validated'  # ✅ Still mark as validated
                }).eq('id', email_record['id']).execute()
    
                if update_result.data:
                    log(f"[SUCCESS] Updated JSON payload for email: {email_file_name} (no attachments - fileBinary set to null)")
                    return True
                else:
                    log(f"[ERROR] Failed to update database for email: {email_file_name}")
                    supabase_write.table('emails').update({
                        'payload_status': 'failed'
                    }).eq('id', email_record['id']).execute()
                    return False
            
            file_binary= cls.encode_zip_to_base64(zip_data)

            if not file_binary:
                log(f"[ERROR] Failed to encode ZIP for email: {email_file_name}")
                # Update status to failed
                supabase_write.table('emails').update({'payload_status': 'failed'}).eq('id', email_record['id']).execute()
                return False
            
            # Update JSON payload with file binary and filename as email name
            email_base_name= email_file_name.replace('.msg', '').replace('.eml', '')
            json_payload['fileBinary'] = file_binary
            json_payload['fileName'] = f"{email_base_name}_attachments.zip"
            #json_payload['fileCount'] = file_count
            json_payload['encoding'] = 'application/zip'

            # Replace documentType with classification_check
            classification_check = email_record.get('classification_check')
            if classification_check:
                json_payload['documentType'] = classification_check

            # ✅ Format the payload to ensure correct field order
            formatted_payload = format_bestmed_payload(json_payload)

            update_result= supabase_write.table('emails').update({
                'json_payload': json.dumps(formatted_payload),
                'payload_status': 'validated'
            }).eq('id', email_record['id']).execute()

            if update_result.data:
                log(f"[SUCCESS] Updated JSON payload for email: {email_file_name} with file binary")
                return True
            else:
                log(f"[ERROR] Failed to update database for email: {email_file_name}")
                supabase_write.table('emails').update({
                    'payload_status': 'failed'
                }).eq('id', email_record['id']).execute()
                return False                

        except Exception as e:
            log(f"[ERROR] Failed to process JSON payload for {email_file_name}: {e}")
            import traceback
            traceback.print_exc()
            # update status to failed
            try:
                result = supabase_read.table('emails').select('id').eq('file_name', email_file_name).execute()
                if result.data:
                    supabase_write.table('emails').update({
                        'payload_status': 'failed'
                    }).eq('id', result.data[0]['id']).execute()
            except:
                pass
            return False   

    @classmethod
    def process_all_emails_with_json_payloads(cls):
        """Process all emails that have JSON payloads but missing file binary"""
        try:
            # Get all emails with JSON payloads
            result = supabase_read.table('emails').select(
                'file_name, json_payload'
            ).not_.is_('json_payload', 'null').execute()
            
            if not result.data:
                log("[INFO] No emails with JSON payloads found")
                return {"processed": 0, "errors": 0}
            
            processed_count = 0
            error_count = 0
            skipped_count = 0
            
            for email_record in result.data:
                email_file_name = email_record['file_name']
                json_payload = email_record.get('json_payload')
                
                # Skip if already has file binary
                if isinstance(json_payload, str):
                    json_payload = json.loads(json_payload)
                
                if json_payload.get('fileBinary') and len(json_payload.get('fileBinary', '')) > 100:
                    log(f"[SKIP] Email {email_file_name} already has file binary")
                    skipped_count += 1
                    continue
                
                # Process this email
                success = cls.process_email_json_payload(email_file_name)
                
                if success:
                    processed_count += 1
                    log(f"[SUCCESS] Processed {email_file_name}")
                else:
                    error_count += 1
                    log(f"[ERROR] Failed to process {email_file_name}")
            
            log(f"[COMPLETE] Processed {processed_count} emails, {error_count} errors")
            return {"processed": processed_count, "errors": error_count, "skipped": skipped_count}
            
        except Exception as e:
            log(f"[ERROR] Failed to process emails with JSON payloads: {e}")
            import traceback
            traceback.print_exc()
            return {"processed": 0, "errors": 1, "skipped": 0}

## Class ends here
    
class PayloadValidator:
    """Validate JSON payloads and set final_classification based on required fields"""
    
    # Validation rules - can match by code OR full document type name
    VALIDATION_RULES = {
        # Code: required fields
        'ADDDEP': ['schemeCode', 'member'],
        'ADDITIONAL DEPENDANT APPLICATION': ['schemeCode', 'member'],
        
        'BANP': ['member'],
        'BACK AND NECK PROGRAMME (NO WORKFLOW)': ['member'],
        
        'BRACCNW': ['schemeCode', 'intermediary'],
        'BROKER ACCREDITATION (NO WORKFLOW)': ['schemeCode', 'intermediary'],
        
        'BRBBBECER': ['schemeCode', 'intermediary'],
        'BROKER BBBEE CERTIFICATE (NO WORKFLOW)': ['schemeCode', 'intermediary'],
        
        'BRKCANW': ['schemeCode', 'intermediary'],
        'BROKER CONTRACT (NO WORKFLOW)': ['schemeCode', 'intermediary'],
        
        'BRKCON': ['schemeCode', 'intermediary'],
        'BROKER CONTRACT': ['schemeCode', 'intermediary'],
        
        'BROKERVAT': ['schemeCode', 'intermediary'],
        'BROKER VAT (NO WORKFLOW)': ['schemeCode', 'intermediary'],
        
        'GRAB_BROK': ['intermediary'],
        'GRABIT BROKER INDEX (NO WORKFLOW)': ['intermediary'],
        
        'MEMBROKCHNG': ['schemeCode', 'member', 'intermediary'],
        'MEMBERSHIP BROKER APPOINTMENT': ['schemeCode', 'member', 'intermediary'],
        
        'CLAIM': ['member', 'provider'],
        'CLAIM ACCOUNTS': ['member', 'provider'],
        
        'FORCLM': [],
        'FOREIGN CLAIM': [],
        
        'FRAUDCC': [],
        'FRAUD CASE CLOSED': [],
        
        'FRAUDR': [],
        'FRAUD REPORT': [],
        
        'FRAUDTP': [],
        'FRAUD TIP OFFS': [],
        
        'PROBANK': ['provider'],
        'PROVIDER BANK CHANGE': ['provider'],
        
        'DISI': ['schemeCode', 'member', 'provider'],
        'COMPLETED SARS DISABILITY FORM': ['schemeCode', 'member', 'provider'],
        
        'CONDEUP': ['schemeCode', 'member'],
        'CONTACT DETAILS UPDATE': ['schemeCode', 'member'],
        
        'DIAL': ['member'],
        'DIALYSIS (NO WORKFLOW)': ['member'],
        
        'DEBT_ESENQ': ['member'],
        'DEBTORS ESTATE LATE ENQUIRY (NO WORKFLOW)': ['member'],
        
        'DEBT_ESLA': ['member'],
        'DEBTORS ESTATE LATE TAX CERTIFICATE (NO WORKFLOW)': ['member'],
        
        'DEBT_CLDEN': ['member'],
        'DEBTORS MEMBER CLAIMS DEBT ENQUIRY (NO WORKFLOW)': ['member'],
        
        'DEBT_MPA': ['member'],
        'DEBTORS MEMBER PAYMENT ARRANGEMENT (NO WORKFLOW)': ['member'],
        
        'DEBT_MPOP': ['member'],
        'DEBTORS MEMBER PROOF OF PAYMENT (NO WORKFLOW)': ['member'],
        
        'DEBT_PREM': ['member'],
        'DEBTORS – PREMIUM RECON (NO WORKFLOW)': ['member'],
        
        'DEBT_PRMRE': [],
        'DEBTORS PREMIUM REFUND (NO WORKFLOW)': [],
        
        'DEBT_SCSA': ['thirdParty'],
        'DEBTORS SCS CLOSED ACCOUNT REPORT (NO WORKFLOW)': ['thirdParty'],
        
        'DEBT_TMDEN': ['member'],
        'DEBTORS TERMINATED MEMBER DEBT ENQUIRY (NO WORKFLOW)': ['member'],
        
        'GRAB_TP': ['thirdParty'],
        'GRABIT THIRD PARTIES (NO WORKFLOW)': ['thirdParty'],
        
        'HOSPUPD': ['member', 'intermediary'],
        'HOSPITAL UPDATES': ['member', 'intermediary'],
        
        'HEARAID': ['member'],
        'HEARING AID AUTH REQUEST': ['member'],
        
        'HIV': ['member'],
        'HIV (NO WORKFLOW)': ['member'],
        
        'ITRDD': ['schemeCode'],
        'ITRDD FORM': ['schemeCode'],
        
        'MATENQ': ['member'],
        'MATERNITY ENQUIRY': ['member'],
        
        'PATOREG': ['schemeCode'],
        'MEMBERSHIP APPLICATION FORM': ['schemeCode'],
        
        'MEMBANK': ['member'],
        'MEMBER BANK CHANGE': ['member'],
        
        'MEMENGMNTW': ['schemeCode', 'member'],
        'MEMBER ENGAGEMENT': ['schemeCode', 'member'],
        
        'PROOFPAY': ['member'],
        'MEMBER PREMIUM PROOF OF PAYMENT': ['member'],
        
        'MEMA': [],
        'MEMBERSHIP ACCEPTANCE LETTER': [],
        
        'PATOCHG': ['member'],
        'MEMBERSHIP CHANGE FORM': ['member'],
        
        'EKPA': [],
        'ESKOM PAYROLL (NO WORKFLOW)': [],
        
        'GRAB_PAYER': ['payer'],
        'GRABIT PAYER INDEX (NO WORKFLOW)': ['payer'],
        
        'MANCERT': ['schemeCode', 'member'],
        'MANUAL CERTIFICATE (NO WORKFLOW)': ['schemeCode', 'member'],
        
        'GRAB_PBIL': ['payer'],
        'PAYER - BILLING INVOICE (NO WORKFLOW)': ['payer'],
        
        'GRAB_PDLF': ['payer'],
        'PAYER - DATA LOAD FILE (NO WORKFLOW)': ['payer'],
        
        'GRAB_PINT': ['payer'],
        'PAYER - INTERFACE (NO WORKFLOW)': ['payer'],
        
        'GRAB_PPSCH': ['payer'],
        'PAYER - PAYMENT SCHEDULE (NO WORKFLOW)': ['payer'],
        
        'PAYANNCOM': ['schemeCode', 'payer'],
        'PAYER ANNUAL COMMUNICATIONS INCREASES (NO WORKFLOW)': ['schemeCode', 'payer'],
        
        'GATOCHG': ['schemeCode', 'payer'],
        'PREMIUM PAYER CHANGE FORM': ['schemeCode', 'payer'],
        
        'MEM_SUP': ['member'],
        'MEMBERSHIP SUPPORTING DOCUMENT': ['member'],
        
        'RETOPT': ['schemeCode', 'member'],
        'MID YEAR OPTION CHANGE': ['schemeCode', 'member'],
        
        'MIDYPNW': ['schemeCode', 'member'],
        'MID YEAR OPTION CHANGE (NO WORKFLOW)': ['schemeCode', 'member'],
        
        'MIDYECC': ['schemeCode', 'member'],
        'MID YEAR OPTION CHANGE CORRESPONDENCE': ['schemeCode', 'member'],
        
        'ONLINE_APP': ['member'],
        'ONLINE MEMBERSHIP APPLICATION FORM': ['member'],
        
        'GATOREG': ['schemeCode'],
        'PREMIUM PAYER APPLICATION FORM': ['schemeCode'],
        
        'ONCO': ['member'],
        'ONCOLOGY (NO WORKFLOW)': ['member'],
        
        'OPCHG': ['member'],
        'OPTION CHANGE': ['member'],
        
        'PREAUTH': ['member'],
        'PRE-AUTHORISATION REQUEST': ['member'],
        
        'PREMENQ': ['schemeCode'],
        'PREMIUM ENQUIRIES': ['schemeCode'],
        
        'TMSTUD': ['member'],
        'PROOF OF STUDENT': ['member'],
        
        'SCANS': ['member', 'provider'],
        'SCANS': ['member', 'provider'],
        
        'DEBT_SPREN': ['provider'],
        'DEBTORS SCS PROVIDER ENQUIRY (NO WORKFLOW)': ['provider'],
        
        'SPCONTRACT': ['provider'],
        'PROVIDER CONTRACT': ['provider'],
        
        'PROVPOI': [],
        'PROVIDER POPI COMPLIANCE ADDENDUM': [],
        
        'MEMTERM': ['schemeCode', 'member'],
        'TERMINATION OF DEPENDANT / MEMBERSHIP': ['schemeCode', 'member'],
        
        'TERMSAVREF': [],
        'TERMINATION SAVINGS REFUND': [],
        
        'UNINDEXEDW': [],
        'UNINDEXED WORKFLOWS': []
    }


    # Reverse mapping: Full name -> Code
    DOCUMENT_TYPE_TO_CODE = {
        'ADDITIONAL DEPENDANT APPLICATION': 'ADDDEP',
        'ADDITIONAL DEPENDENT': 'ADDDEP',
        'BACK AND NECK PROGRAMME (NO WORKFLOW)': 'BANP',
        'BROKER ACCREDITATION (NO WORKFLOW)': 'BRACCNW',
        'BROKER BBBEE CERTIFICATE (NO WORKFLOW)': 'BRBBBECER',
        'BROKER CONTRACT (NO WORKFLOW)': 'BRKCANW',
        'BROKER CONTRACT': 'BRKCON',
        'BROKER VAT (NO WORKFLOW)': 'BROKERVAT',
        'GRABIT BROKER INDEX (NO WORKFLOW)': 'GRAB_BROK',
        'MEMBERSHIP BROKER APPOINTMENT': 'MEMBROKCHNG',
        'CLAIM ACCOUNTS': 'CLAIM',
        'FOREIGN CLAIM': 'FORCLM',
        'FRAUD CASE CLOSED': 'FRAUDCC',
        'FRAUD REPORT': 'FRAUDR',
        'FRAUD TIP OFFS': 'FRAUDTP',
        'PROVIDER BANK CHANGE': 'PROBANK',
        'COMPLETED SARS DISABILITY FORM': 'DISI',
        'CONTACT DETAILS UPDATE': 'CONDEUP',
        'DIALYSIS (NO WORKFLOW)': 'DIAL',
        'DEBTORS ESTATE LATE ENQUIRY (NO WORKFLOW)': 'DEBT_ESENQ',
        'DEBTORS ESTATE LATE TAX CERTIFICATE (NO WORKFLOW)': 'DEBT_ESLA',
        'DEBTORS MEMBER CLAIMS DEBT ENQUIRY (NO WORKFLOW)': 'DEBT_CLDEN',
        'DEBTORS MEMBER PAYMENT ARRANGEMENT (NO WORKFLOW)': 'DEBT_MPA',
        'DEBTORS MEMBER PROOF OF PAYMENT (NO WORKFLOW)': 'DEBT_MPOP',
        'DEBTORS – PREMIUM RECON (NO WORKFLOW)': 'DEBT_PREM',
        'DEBTORS PREMIUM REFUND (NO WORKFLOW)': 'DEBT_PRMRE',
        'DEBTORS SCS CLOSED ACCOUNT REPORT (NO WORKFLOW)': 'DEBT_SCSA',
        'DEBTORS TERMINATED MEMBER DEBT ENQUIRY (NO WORKFLOW)': 'DEBT_TMDEN',
        'GRABIT THIRD PARTIES (NO WORKFLOW)': 'GRAB_TP',
        'HOSPITAL UPDATES': 'HOSPUPD',
        'HEARING AID AUTH REQUEST': 'HEARAID',
        'HIV (NO WORKFLOW)': 'HIV',
        'ITRDD FORM': 'ITRDD',
        'MATERNITY ENQUIRY': 'MATENQ',
        'MEMBERSHIP APPLICATION FORM': 'PATOREG',
        'MEMBER BANK CHANGE': 'MEMBANK',
        'MEMBER ENGAGEMENT': 'MEMENGMNTW',
        'MEMBER PREMIUM PROOF OF PAYMENT': 'PROOFPAY',
        'MEMBERSHIP ACCEPTANCE LETTER': 'MEMA',
        'MEMBERSHIP CHANGE FORM': 'PATOCHG',
        'ESKOM PAYROLL (NO WORKFLOW)': 'EKPA',
        'GRABIT PAYER INDEX (NO WORKFLOW)': 'GRAB_PAYER',
        'MANUAL CERTIFICATE (NO WORKFLOW)': 'MANCERT',
        'PAYER - BILLING INVOICE (NO WORKFLOW)': 'GRAB_PBIL',
        'PAYER - DATA LOAD FILE (NO WORKFLOW)': 'GRAB_PDLF',
        'PAYER - INTERFACE (NO WORKFLOW)': 'GRAB_PINT',
        'PAYER - PAYMENT SCHEDULE (NO WORKFLOW)': 'GRAB_PPSCH',
        'PAYER ANNUAL COMMUNICATIONS INCREASES (NO WORKFLOW)': 'PAYANNCOM',
        'PREMIUM PAYER CHANGE FORM': 'GATOCHG',
        'MEMBERSHIP SUPPORTING DOCUMENT': 'MEM_SUP',
        'MID YEAR OPTION CHANGE': 'RETOPT',
        'MID YEAR OPTION CHANGE (NO WORKFLOW)': 'MIDYPNW',
        'MID YEAR OPTION CHANGE CORRESPONDENCE': 'MIDYECC',
        'ONLINE MEMBERSHIP APPLICATION FORM': 'ONLINE_APP',
        'PREMIUM PAYER APPLICATION FORM': 'GATOREG',
        'ONCOLOGY (NO WORKFLOW)': 'ONCO',
        'OPTION CHANGE': 'OPCHG',
        'PRE-AUTHORISATION REQUEST': 'PREAUTH',
        'PREMIUM ENQUIRIES': 'PREMENQ',
        'PROOF OF STUDENT': 'TMSTUD',
        'SCANS': 'SCANS',
        'DEBTORS SCS PROVIDER ENQUIRY (NO WORKFLOW)': 'DEBT_SPREN',
        'PROVIDER CONTRACT': 'SPCONTRACT',
        'PROVIDER POPI COMPLIANCE ADDENDUM': 'PROVPOI',
        'TERMINATION OF DEPENDANT / MEMBERSHIP': 'MEMTERM',
        'TERMINATION SAVINGS REFUND': 'TERMSAVREF',
        'UNINDEXED WORKFLOWS': 'UNINDEXEDW'
    }

    @classmethod
    def get_document_type_code(cls, full_name):
        """Convert full document type name to code"""
        code = cls.DOCUMENT_TYPE_TO_CODE.get(full_name.upper(), full_name)
        log(f"[CODE MAPPING] '{full_name}' -> '{code}'")
        return code
    
    @classmethod
    def is_field_populated(cls, field_value):
        """Check if a field has a valid value"""
        if field_value is None:
            return False
        if isinstance(field_value, str):
            field_value = field_value.strip()
            if field_value == '' or field_value.upper() == 'N':
                return False
        return True
    
    @classmethod
    def validate_email(cls, email_file_name):
        """
        Validate email and set final_classification
        - If all required fields present: final_classification = documentType
        - If missing fields: final_classification = "UNINDEXED WORKFLOWS"
        """
        try:
            # Get email record
            result = supabase_read.table('emails').select(
                'id, file_name, json_payload, classification_check'
            ).eq('file_name', email_file_name).execute()
            
            if not result.data:
                log(f"[VALIDATION] Email not found: {email_file_name}")
                return {'status': 'error', 'message': f'Email not found: {email_file_name}'}
            
            email_record = result.data[0]
            json_payload = email_record.get('json_payload')
            
            if not json_payload:
                log(f"[VALIDATION] No JSON payload for: {email_file_name}")
                return {'status': 'error', 'message': 'No JSON payload found'}
            
            # Parse JSON if string
            if isinstance(json_payload, str):
                json_payload = json.loads(json_payload)
            
            document_type = json_payload.get('documentType', '').upper()

            
            # Check if document type exists in rules
            if document_type not in cls.VALIDATION_RULES:
                log(f"[VALIDATION] Unknown document type '{document_type}' for {email_file_name}")
                
                # ✅ Update JSON payload's documentType to UNINDEXED WORKFLOWS
                json_payload['documentType'] = 'UNINDEXEDW'
                formatted_payload = format_bestmed_payload(json_payload)

                # ✅ Update database with final_classification AND updated JSON payload
                supabase_write.table('emails').update({
                    'final_classification': final_classification,
                    'json_payload': json.dumps(formatted_payload)  # ✅ ADD THIS LINE
                }).eq('id', email_record['id']).execute()

                log(f"[VALIDATION] ✅ Updated documentType in JSON payload to 'UNINDEXEDW' for {email_file_name}")
                
                return {
                    'status': 'success',
                    'email_file_name': email_file_name,
                    'original_classification': email_record.get('classification_check'),
                    'document_type': document_type,
                    'is_valid': False,
                    'final_classification': 'UNINDEXED WORKFLOWS',
                    'reason': 'Unknown document type'
                }
            
            # Get required fields for this document type
            required_fields = cls.VALIDATION_RULES[document_type]
            missing_fields = []
            
            # Check each required field
            for field_name in required_fields:
                field_value = json_payload.get(field_name)
                if not cls.is_field_populated(field_value):
                    missing_fields.append(field_name)
            
            # Determine final classification
            if len(missing_fields) == 0:
                final_classification = document_type
                is_valid = True
                log(f"[VALIDATION] ✅ VALID - {email_file_name}: {document_type} (all fields present)")
            else:
                final_classification = 'UNINDEXED WORKFLOWS'
                is_valid = False
                log(f"[VALIDATION] ❌ INVALID - {email_file_name}: {document_type} → UNINDEXED WORKFLOWS")
                log(f"[VALIDATION] Missing fields: {missing_fields}")

            # ✅ Update JSON payload's documentType with CODE (not full name)
            document_type_code = cls.get_document_type_code(final_classification)
            #✅ Update JSON payload's documentType with final_classification
            json_payload['documentType'] = document_type_code
            formatted_payload = format_bestmed_payload(json_payload)
            
            # ✅ Update database with final_classification AND updated JSON payload
            supabase_write.table('emails').update({
                'final_classification': final_classification,
                'json_payload': json.dumps(formatted_payload)  # ✅ ADD THIS LINE
            }).eq('id', email_record['id']).execute()
        
            log(f"[VALIDATION] Set final_classification = '{final_classification}' for {email_file_name}")
            log(f"[VALIDATION] ✅ Updated documentType in JSON payload to '{final_classification}' for {email_file_name}")
        
            return {
                'status': 'success',
                'email_file_name': email_file_name,
                'original_classification': email_record.get('classification_check'),
                'document_type': document_type,
                'is_valid': is_valid,
                'final_classification': final_classification,
                'missing_fields': missing_fields
            }
            
        except Exception as e:
            log(f"[ERROR] Validation failed for {email_file_name}: {e}")
            import traceback
            traceback.print_exc()
            return {'status': 'error', 'message': str(e)}
    
    @classmethod
    def validate_all(cls):
        """Validate all emails with JSON payloads"""
        try:
            log("[VALIDATION] Starting validation of all emails...")
            
            # Get all emails with JSON payloads
            result = supabase_read.table('emails').select(
                'file_name'
            ).not_.is_('json_payload', 'null').execute()
            
            if not result.data:
                log("[VALIDATION] No emails with JSON payloads found")
                return {
                    'status': 'error',
                    'message': 'No emails with JSON payloads found'
                }
            
            total = len(result.data)
            valid_count = 0
            invalid_count = 0
            error_count = 0
            
            log(f"[VALIDATION] Found {total} emails to validate")
            
            for email_record in result.data:
                email_file_name = email_record['file_name']
                validation_result = cls.validate_email(email_file_name)
                
                if validation_result['status'] == 'success':
                    if validation_result['is_valid']:
                        valid_count += 1
                    else:
                        invalid_count += 1
                else:
                    error_count += 1
            
            log(f"[VALIDATION] ✅ Complete! Valid: {valid_count}, Invalid (UNINDEXED): {invalid_count}, Errors: {error_count}")
            
            return {
                'status': 'success',
                'total': total,
                'valid': valid_count,
                'invalid': invalid_count,
                'errors': error_count
            }
            
        except Exception as e:
            log(f"[ERROR] Failed to validate all emails: {e}")
            import traceback
            traceback.print_exc()
            return {'status': 'error', 'message': str(e)}

def log(msg):
    print(msg, flush=True)
    log_queue.put(msg + "\n")

def extract_excel_content(file_data, filename):
    """
    Extract content from Excel files (.xlsx, .xls) and return as JSON string
    Returns None if extraction fails or file is not Excel
    """
    file_ext = filename.split('.')[-1].lower() if '.' in filename else ''
    
    if file_ext not in ['xlsx', 'xls', 'xlsm', 'xlsb']:
        return None
    
    try:
        
        # Read Excel file into pandas
        excel_file = BytesIO(file_data)
        
        # Read all sheets
        excel_data = pd.read_excel(excel_file, sheet_name=None, engine='openpyxl' if file_ext == 'xlsx' else 'xlrd')
        
        # Convert to JSON-serializable format
        result = {}
        for sheet_name, df in excel_data.items():
            # Replace NaN with None for JSON serialization
            df = df.where(pd.notna(df), None)
            result[sheet_name] = {
                'columns': df.columns.tolist(),
                'data': df.values.tolist(),
                'row_count': len(df),
                'column_count': len(df.columns)
            }
        
        
        content_json = json.dumps(result, indent=2, default=str)
        log(f"[EXCEL] Successfully extracted content from {filename} ({len(excel_data)} sheets)")
        return content_json
        
    except ImportError as e:
        log(f"[ERROR] Missing required library for Excel processing: {e}")
        log(f"[INFO] Please install: pip install pandas openpyxl xlrd")
        return None
    except Exception as e:
        log(f"[ERROR] Failed to extract Excel content from {filename}: {e}")
        return None

# --- DATABASE HELPER FUNCTIONS ---
def get_mime_type(filename, file_data=None):
    """
    Detect the correct MIME type for a file based on its extension and content.
    Returns the MIME type string.
    """
    # Get MIME type from filename extension
    mime_type, _ = mimetypes.guess_type(filename)
    
    # If mimetypes didn't recognize it, try common extensions manually
    if not mime_type:
        ext = filename.split('.')[-1].lower() if '.' in filename else ''
        mime_map = {
            'pdf': 'application/pdf',
            'doc': 'application/msword',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'xls': 'application/vnd.ms-excel',
            'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'ppt': 'application/vnd.ms-powerpoint',
            'pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'png': 'image/png',
            'gif': 'image/gif',
            'bmp': 'image/bmp',
            'tiff': 'image/tiff',
            'tif': 'image/tiff',
            'svg': 'image/svg+xml',
            'txt': 'text/plain',
            'csv': 'text/csv',
            'html': 'text/html',
            'htm': 'text/html',
            'xml': 'text/xml',
            'json': 'application/json',
            'zip': 'application/zip',
            'rar': 'application/x-rar-compressed',
            '7z': 'application/x-7z-compressed',
            'tar': 'application/x-tar',
            'gz': 'application/gzip',
            'mp3': 'audio/mpeg',
            'wav': 'audio/wav',
            'mp4': 'video/mp4',
            'avi': 'video/x-msvideo',
            'mov': 'video/quicktime',
            'msg': 'application/vnd.ms-outlook',
            'eml': 'message/rfc822'
        }
        mime_type = mime_map.get(ext, 'application/octet-stream')
    
    return mime_type

def format_bestmed_payload(payload: dict) -> dict:
    """Format JSON payload into the standard Bestmed structure with correct field order."""
    # Return a clean ordered dict matching your desired schema
    return {
        "member": payload.get("member"),
        "schemeCode": payload.get("schemeCode"),
        "correspondenceDate": payload.get("correspondenceDate"),
        "expiryDate": payload.get("expiryDate"),
        "provider": payload.get("provider"),
        "barcode": payload.get("barcode"),
        "documentType": payload.get("documentType"),
        "documentSubType": payload.get("documentSubType"),
        "payer": payload.get("payer"),
        "intermediary": payload.get("intermediary"),
        "source": payload.get("source"),
        "encoding": payload.get("encoding"),
        "carbonCopy": payload.get("carbonCopy"),
        "physicalLocation": payload.get("physicalLocation"),
        "thirdParty": payload.get("thirdParty"),
        "dependantNo": payload.get("dependantNo"),
        "entityType": payload.get("entityType"),
        "fileName": payload.get("fileName"),
        "fileBinary": payload.get("fileBinary"),
        "externalReference": payload.get("externalReference"),  # ✅ Added missing field
        "cellPhoneNumber": payload.get("cellPhoneNumber"),
        "priority": payload.get("priority"),
        "note": payload.get("note"),
        "dueDate": payload.get("dueDate"),
        "schemeAuthorisationReference": payload.get("schemeAuthorisationReference")
    }


def insert_email_to_db(email_data):
    """Insert email record into Supabase database using service role key"""
    try:
        # event ids funcitionality
        if 'event_id' not in email_data or not email_data['event_id']:
            email_data['event_id'] = BestmedPayloadProcessor.generate_event_id()
            log(f"[DB] Generated Event ID: {email_data['event_id']} for email: {email_data.get('file_name')}")

        if 'payload_status' not in email_data:
            email_data['payload_status'] = 'pending'

        result = supabase_write.table('emails').insert(email_data).execute()
        log(f"[DB] Inserted email record: {email_data.get('file_name')} with Event ID: {email_data.get('event_id')}")
        return result.data[0]['id'] if result.data else None
    except Exception as e:
        error_str = str(e).lower()
        if 'duplicate' in error_str or 'unique' in error_str:
            log(f"[SKIP] Email already exists in database: {email_data.get('file_name')}")
            return None
        else:
            log(f"[ERROR] Failed to insert email to database: {e}")
            raise

def insert_attachments_to_db(email_id, attachments_list):
    """Insert attachment records into Supabase database using service role key"""
    if not attachments_list or not email_id:
        return
    try:
        supabase_write.table('attachments').insert(attachments_list).execute()
        log(f"[DB] Inserted {len(attachments_list)} attachment records")
    except Exception as e:
        log(f"[ERROR] Failed to insert attachments to database: {e}")
        raise

def get_processed_files():
    """Query database for already processed email files using anon key"""
    try:
        result = supabase_read.table('emails').select('file_name, folder').execute()
        processed = {(r['file_name'], r['folder']) for r in result.data}
        log(f"[DB] Found {len(processed)} already processed emails")
        return processed
    except Exception as e:
        log(f"[ERROR] Failed to query processed files: {e}")
        return set()

# --- HELPER FUNCTIONS ---

def sanitize_filename(name):
    """Remove problematic characters from filenames"""
    if not name:
        return "unnamed_file"
    
    # Remove null bytes and problematic Unicode characters
    name = re.sub(r'[\u0000\u200B-\u200F\u202A-\u202E\u00A0]', '', name)
    
    # Remove filesystem-incompatible characters AND square brackets
    name = "".join(c for c in name if c not in r'<>:"/\|?*[]').strip().rstrip("\\/")
    
    return name if name else "unnamed_file"

def upload_to_supabase(bucket, path, data, content_type=None):
    """Upload file to Supabase storage only if it doesn't already exist"""
    if isinstance(data, str):
        data = data.encode("utf-8")
    
    # Auto-detect MIME type if not provided
    if not content_type:
        filename = os.path.basename(path)
        content_type = get_mime_type(filename, data)
        log(f"[DEBUG] Auto-detected MIME type for '{filename}': {content_type}")
    
    file_options = {"content-type": content_type}
    
    # Check if file already exists
    try:
        existing_files = supabase_read.storage.from_(bucket).list(path=os.path.dirname(path))
        filename = os.path.basename(path)
        
        # Check if this specific file exists
        file_exists = any(f['name'] == filename for f in existing_files)
        
        if file_exists:
            log(f"[INFO] File '{path}' already exists, skipping upload.")
            return None
    except Exception as e:
        log(f"[WARN] Could not check if file exists: {e}. Proceeding with upload.")
    
    # Upload new file
    try:
        res = supabase_read.storage.from_(bucket).upload(path, data, file_options=file_options)
        log(f"[DEBUG] Uploaded '{path}' successfully with MIME type: {content_type}.")
        return res
    except Exception as e:
        log(f"[ERROR] Upload failed for '{path}': {e}")
        return None


def extract_eml_content(eml_bytes):
    """
    Extract content from .eml files
    Returns dict with sender, date, subject, body, and attachments
    """
    try:
        msg = email.message_from_bytes(eml_bytes, policy=policy.default)
        
        sender = msg.get('From', 'Unknown')
        date = msg.get('Date', None)
        subject = msg.get('Subject', 'No Subject')
        to = msg.get('To', '')
        cc = msg.get('Cc', '')
        
        # Extract body
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    body = part.get_payload(decode=True).decode('utf-8', errors='ignore')
                    break
        else:
            body = msg.get_payload(decode=True).decode('utf-8', errors='ignore')


        # Extract attachments (including inline images)
        attachments = []
        if msg.is_multipart():
            for part in msg.walk():
                content_disposition = part.get_content_disposition()
                content_type = part.get_content_type()
                
                # Check if it's an attachment or inline image
                if content_disposition in ['attachment', 'inline'] or content_type.startswith('image/'):
                    filename = part.get_filename()
                    
                    # For inline images without filename, generate one based on Content-ID or index
                    if not filename and content_type.startswith('image/'):
                        content_id = part.get('Content-ID', '').strip('<>')
                        if content_id:
                            # Use Content-ID as filename
                            filename = f"inline_{content_id}"
                        else:
                            # Generate filename based on content type
                            ext = content_type.split('/')[-1]
                            filename = f"inline_image_{len(attachments)}.{ext}"
                    
                    if filename:
                        attachments.append({
                            'filename': filename,
                            'data': part.get_payload(decode=True),
                            'content_type': content_type,
                            'is_inline': content_disposition == 'inline' or (not content_disposition and content_type.startswith('image/'))
                        })
        
        return {
            'sender': sender,
            'date': date,
            'subject': subject,
            'to': to,
            'cc': cc,
            'body': body,
            'attachments': attachments
        }
    except Exception as e:
        log(f"[ERROR] Failed to extract EML content: {e}")
        return None

def process_nested_email(file_data, filename, parent_email_dir, parent_email_id, attachment_index):
    """
    Process nested .msg or .eml files found as attachments
    Returns list of attachment records for database
    """
    file_ext = filename.split('.')[-1].lower() if '.' in filename else ''
    attachment_data_list = []
    
    try:
        if file_ext == 'msg':
            # Process nested MSG file
            nested_msg = extract_msg.Message(BytesIO(file_data))
            
            sender = nested_msg.sender or "Unknown"
            date = nested_msg.date or None
            subject = nested_msg.subject or "No Subject"
            body = nested_msg.body or ""
            
            # Clean body
            body = re.sub(r'[\u0000\u200B-\u200F\u202A-\u202E\u00A0]', ' ', body)
            
            # Extract inline images from nested MSG BEFORE processing attachments
            if hasattr(nested_msg, 'htmlBody') and nested_msg.htmlBody:
                try:
                    # Find all cid: references in HTML
                    cid_pattern = r'cid:([^"\'>\s]+)'
                    cids = re.findall(cid_pattern, nested_msg.htmlBody)
                    
                    # Extract embedded images
                    for cid in cids:
                        # Try to find the attachment with this Content-ID
                        for att_check in nested_msg.attachments:
                            if hasattr(att_check, 'cid') and att_check.cid == cid:
                                # This is an inline image
                                nested_base_name_temp = sanitize_filename(os.path.splitext(filename)[0])
                                inline_filename = sanitize_filename(f"inline_{cid}.png")
                                inline_path = f"{parent_email_dir}/{nested_base_name_temp}_{inline_filename}"
                                
                                if att_check.data:
                                    upload_to_supabase(BUCKET_NAME, inline_path, att_check.data)
                                    
                                    attachment_data_list.append({
                                        'email_id': parent_email_id,
                                        'file_name': f"{nested_base_name_temp}_{inline_filename}",
                                        'file_path': inline_path,
                                        'file_size_bytes': len(att_check.data),
                                        'file_extension': 'png',
                                        'attachment_index': attachment_index,
                                        'attachment_processing': f"Nested inline image (Content-ID: {cid})",
                                        'attachment_validity': None,
                                        'file_url': supabase_read.storage.from_(BUCKET_NAME).get_public_url(inline_path).rstrip('?')
                                    })
                                    log(f"[INFO] Extracted nested inline image: {inline_filename}")
                except Exception as e:
                    log(f"[WARN] Failed to extract inline images from nested MSG: {e}")
            
            # Save nested email body
            nested_base_name = sanitize_filename(os.path.splitext(filename)[0])

            body_path = f"{parent_email_dir}/{nested_base_name}_body.txt"
            upload_to_supabase(BUCKET_NAME, body_path, body.encode('utf-8'), "text/plain")
            
            # Add body to attachments list
            attachment_data_list.append({
                'email_id': parent_email_id,  # Will be updated later if None
                'file_name': f"{nested_base_name}_body.txt",
                'file_path': body_path,
                'file_size_bytes': len(body.encode('utf-8')),
                'file_extension': 'txt',
                'attachment_index': attachment_index,
                'attachment_processing': f"Nested email body from: {filename}\nSubject: {subject}\nFrom: {sender}",
                'attachment_validity': None,  # ✅ Added missing field
                'file_url': supabase_read.storage.from_(BUCKET_NAME).get_public_url(body_path).rstrip('?')
            })
            
            # Process nested attachments
            for idx, att in enumerate(nested_msg.attachments):
                att_filename = sanitize_filename(att.longFilename or att.shortFilename or f"nested_attachment_{idx}")
                if not att_filename or not att.data:
                    continue
                
                att_path = f"{parent_email_dir}/{nested_base_name}_{att_filename}"
                upload_to_supabase(BUCKET_NAME, att_path, att.data)
                
                file_extension = att_filename.split('.')[-1].lower() if '.' in att_filename else ''
                excel_content = None
                if file_extension in ['xlsx', 'xls', 'xlsm', 'xlsb']:
                    excel_content = extract_excel_content(att.data, att_filename)
                
                attachment_data_list.append({
                    'email_id': parent_email_id,  # Will be updated later if None
                    'file_name': att_filename,
                    'file_path': att_path,
                    'file_size_bytes': len(att.data) if att.data else 0,
                    'file_extension': file_extension,
                    'attachment_index': attachment_index,
                    'attachment_processing': excel_content,
                    'attachment_validity': None,  # ✅ Added missing field
                    'file_url': supabase_read.storage.from_(BUCKET_NAME).get_public_url(att_path).rstrip('?')
                })
            
            nested_msg.close()
            
        elif file_ext == 'eml':
            # Process nested EML file
            eml_data = extract_eml_content(file_data)
            if not eml_data:
                return attachment_data_list
            
            # Save nested email body
            nested_base_name = sanitize_filename(os.path.splitext(filename)[0])
            body = eml_data['body']
            body_path = f"{parent_email_dir}/{nested_base_name}_body.txt"
            upload_to_supabase(BUCKET_NAME, body_path, body.encode('utf-8'), "text/plain")
            
            # Add body to attachments list
            attachment_data_list.append({
                'email_id': parent_email_id,  # Will be updated later if None
                'file_name': f"{nested_base_name}_body.txt",
                'file_path': body_path,
                'file_size_bytes': len(body.encode('utf-8')),
                'file_extension': 'txt',
                'attachment_index': attachment_index,
                'attachment_processing': f"Nested email body from: {filename}\nSubject: {eml_data['subject']}\nFrom: {eml_data['sender']}",
                'attachment_validity': None,  # ✅ Added missing field
                'file_url': supabase_read.storage.from_(BUCKET_NAME).get_public_url(body_path).rstrip('?')
            })
            
            # Process nested attachments
            for idx, att in enumerate(eml_data['attachments']):
                att_filename = sanitize_filename(att['filename'])
                att_data = att['data']
                
                att_path = f"{parent_email_dir}/{nested_base_name}_{att_filename}"
                upload_to_supabase(BUCKET_NAME, att_path, att_data)
                
                file_extension = att_filename.split('.')[-1].lower() if '.' in att_filename else ''
                excel_content = None
                if file_extension in ['xlsx', 'xls', 'xlsm', 'xlsb']:
                    excel_content = extract_excel_content(att_data, att_filename)
                
                attachment_data_list.append({
                    'email_id': parent_email_id,  # Will be updated later if None
                    'file_name': att_filename,
                    'file_path': att_path,
                    'file_size_bytes': len(att_data) if att_data else 0,
                    'file_extension': file_extension,
                    'attachment_index': attachment_index,
                    'attachment_processing': excel_content,
                    'attachment_validity': None,  # ✅ Added missing field
                    'file_url': supabase_read.storage.from_(BUCKET_NAME).get_public_url(att_path).rstrip('?')
                })
        
        log(f"[INFO] Successfully processed nested email: {filename}")
        
    except Exception as e:
        log(f"[ERROR] Failed to process nested email {filename}: {e}")
    
    return attachment_data_list

def process_email_file(email_bytes, email_name, folder_name):
    """Process both .msg and .eml files with unified logic"""
    start_time = time.time()
    processing_started_at = datetime.now().isoformat()
    
    # Determine file type
    file_extension = email_name.split('.')[-1].lower() if '.' in email_name else ''

    try:
        # Extract email data based on file type
        if file_extension == 'msg':
            msg = extract_msg.Message(BytesIO(email_bytes))
            msg_sender = msg.sender or "Unknown"
            msg_date = msg.date or None
            msg_subject = msg.subject or "No Subject"
            msg_body = msg.body or ""
            msg_to = msg.to or ""
            msg_cc = msg.cc or ""
            msg_bcc = msg.bcc or ""
            attachments = msg.attachments

            # CRITICAL: Clean ALL text fields immediately to remove null bytes
            msg_sender = re.sub(r'[\u0000\u200B-\u200F\u202A-\u202E\u00A0]', ' ', msg_sender)
            msg_subject = re.sub(r'[\u0000\u200B-\u200F\u202A-\u202E\u00A0]', ' ', msg_subject)
            msg_body = re.sub(r'[\u0000\u200B-\u200F\u202A-\u202E\u00A0]', ' ', msg_body)
            msg_to = re.sub(r'[\u0000\u200B-\u200F\u202A-\u202E\u00A0]', ' ', msg_to)
            msg_cc = re.sub(r'[\u0000\u200B-\u200F\u202A-\u202E\u00A0]', ' ', msg_cc)
            msg_bcc = re.sub(r'[\u0000\u200B-\u200F\u202A-\u202E\u00A0]', ' ', msg_bcc)
            
        elif file_extension == 'eml':
            eml_data = extract_eml_content(email_bytes)
            if not eml_data:
                raise Exception("Failed to extract EML content")
            msg_sender = eml_data['sender']
            msg_date = eml_data['date']
            msg_subject = eml_data['subject']
            msg_body = eml_data['body']
            msg_to = eml_data['to']
            msg_cc = eml_data['cc']
            msg_bcc = ''
            # Convert EML attachments to MSG-like format for unified processing
            attachments = eml_data['attachments']

            # CRITICAL: Clean ALL text fields immediately to remove null bytes
            msg_sender = re.sub(r'[\u0000\u200B-\u200F\u202A-\u202E\u00A0]', ' ', msg_sender)
            msg_subject = re.sub(r'[\u0000\u200B-\u200F\u202A-\u202E\u00A0]', ' ', msg_subject)
            msg_body = re.sub(r'[\u0000\u200B-\u200F\u202A-\u202E\u00A0]', ' ', msg_body)
            msg_to = re.sub(r'[\u0000\u200B-\u200F\u202A-\u202E\u00A0]', ' ', msg_to)
            msg_cc = re.sub(r'[\u0000\u200B-\u200F\u202A-\u202E\u00A0]', ' ', msg_cc)
        else:
            raise Exception(f"Unsupported file type: {file_extension}")
        
        msg_body = re.sub(r'[\u0000\u200B-\u200F\u202A-\u202E\u00A0]', ' ', msg_body)

        email_base_name = sanitize_filename(os.path.splitext(email_name)[0])
        email_dir = f"{OUTPUT_FOLDER}/{folder_name}/{email_base_name}"

        msg_body = re.sub(r'[\u0000\u200B-\u200F\u202A-\u202E\u00A0]', ' ', msg_body)
        
        # Upload email body
        txt_path = f"{email_dir}/{email_base_name}.txt"
        log(f"[INFO] Uploading email body to {txt_path}")
        upload_to_supabase(BUCKET_NAME, txt_path, msg_body.encode('utf-8'), "text/plain")

        # Initialize attachment list with email body
        attachment_data_list = [{
            'file_name': f"{email_base_name}.txt",
            'file_path': txt_path,
            'file_size_bytes': len(msg_body.encode('utf-8')),
            'file_extension': 'txt',
            'attachment_index': -1,
            'attachment_processing': msg_body,
            'attachment_validity': None,
            'file_url': supabase_read.storage.from_(BUCKET_NAME).get_public_url(txt_path).rstrip('?')
        }]

        attachment_paths = [txt_path]

        # Extract inline images from MSG BEFORE processing regular attachments
        if file_extension == 'msg' and hasattr(msg, 'htmlBody') and msg.htmlBody:
            # Check for embedded images in HTML body
            try:
                # Find all cid: references in HTML
                cid_pattern = r'cid:([^"\'>\s]+)'
                cids = re.findall(cid_pattern, msg.htmlBody)
                
                # Extract embedded images
                for cid in cids:
                    # Try to find the attachment with this Content-ID
                    for att_check in msg.attachments:
                        if hasattr(att_check, 'cid') and att_check.cid == cid:
                            # This is an inline image
                            inline_filename = sanitize_filename(f"inline_{cid}.png")
                            inline_path = f"{email_dir}/{inline_filename}"
                            
                            if att_check.data:
                                upload_to_supabase(BUCKET_NAME, inline_path, att_check.data)
                                
                                attachment_data_list.append({
                                    'file_name': inline_filename,
                                    'file_path': inline_path,
                                    'file_size_bytes': len(att_check.data),
                                    'file_extension': 'png',
                                    'attachment_index': -2,  # Special index for inline images
                                    'attachment_processing': f"Inline image (Content-ID: {cid})",
                                    'attachment_validity': None,
                                    'file_url': supabase_read.storage.from_(BUCKET_NAME).get_public_url(inline_path).rstrip('?')
                                })
                                log(f"[INFO] Extracted inline image: {inline_filename}")
            except Exception as e:
                log(f"[WARN] Failed to extract inline images from MSG: {e}")

        # Process attachments (unified for both MSG and EML)
        for idx, att in enumerate(attachments):
            # Handle different attachment structures
            if file_extension == 'msg':


                # Check if attachment is a nested Message object (MSG file embedded)
                if isinstance(att.data, extract_msg.Message):
                    filename = (att.longFilename or att.shortFilename or "attachment")
                    filename = filename if filename.endswith('.msg') else f"{filename}.msg"
                    filename = sanitize_filename(filename)
                    file_path = f"{email_dir}/{filename}"
                    file_extension_att = 'msg'
                    
                    log(f"[INFO] Found nested email (Message object): {filename}")
                    
                    try:
                        nested_msg = att.data
                        nested_sender = nested_msg.sender or "Unknown"
                        nested_date = nested_msg.date or None
                        nested_subject = nested_msg.subject or "No Subject"
                        nested_body = nested_msg.body or ""
                        
                        nested_body = re.sub(r'[\u0000\u200B-\u200F\u202A-\u202E\u00A0]', ' ', nested_body)
                        
                        nested_base_name = sanitize_filename(os.path.splitext(filename)[0])
                        body_path = f"{email_dir}/{nested_base_name}_body.txt"
                        upload_to_supabase(BUCKET_NAME, body_path, nested_body.encode('utf-8'), "text/plain")
                        
                        attachment_data_list.append({
                            'file_name': f"{nested_base_name}_body.txt",
                            'file_path': body_path,
                            'file_size_bytes': len(nested_body.encode('utf-8')),
                            'file_extension': 'txt',
                            'attachment_index': idx,
                            'attachment_processing': f"Nested email body from: {filename}\nSubject: {nested_subject}\nFrom: {nested_sender}\nDate: {nested_date}",
                            'attachment_validity': None,
                            'file_url': supabase_read.storage.from_(BUCKET_NAME).get_public_url(body_path).rstrip('?')
                        })
                        
                        # Process nested attachments
                        for nested_idx, nested_att in enumerate(nested_msg.attachments):
                            nested_att_filename = sanitize_filename(
                                nested_att.longFilename or nested_att.shortFilename or f"nested_attachment_{nested_idx}"
                            )
                            if not nested_att_filename or not nested_att.data:
                                continue
                            
                            nested_att_path = f"{email_dir}/{nested_base_name}_{nested_att_filename}"
                            
                            if isinstance(nested_att.data, extract_msg.Message):
                                log(f"[WARN] Skipping deeply nested email: {nested_att_filename} (not supported)")
                                continue
                            
                            upload_to_supabase(BUCKET_NAME, nested_att_path, nested_att.data)
                            
                            nested_file_extension = nested_att_filename.split('.')[-1].lower() if '.' in nested_att_filename else ''
                            excel_content = None
                            if nested_file_extension in ['xlsx', 'xls', 'xlsm', 'xlsb']:
                                excel_content = extract_excel_content(nested_att.data, nested_att_filename)
                            
                            attachment_data_list.append({
                                'file_name': f"{nested_base_name}_{nested_att_filename}",
                                'file_path': nested_att_path,
                                'file_size_bytes': len(nested_att.data) if nested_att.data else 0,
                                'file_extension': nested_file_extension,
                                'attachment_index': idx,
                                'attachment_processing': excel_content,
                                'attachment_validity': None,
                                'file_url': supabase_read.storage.from_(BUCKET_NAME).get_public_url(nested_att_path).rstrip('?')
                            })
                        
                        log(f"[INFO] Successfully processed nested email: {filename} ({len(nested_msg.attachments)} attachments)")
                        
                    except Exception as e:
                        log(f"[ERROR] Failed to process nested Message object {filename}: {e}")
                    
                    continue
                
                # Regular MSG attachment
                filename = sanitize_filename(att.longFilename or att.shortFilename or f"attachment_{idx}")
                att_data = att.data
                
            
            elif file_extension == 'eml':
                # EML attachment structure
                filename = sanitize_filename(att['filename'])
                att_data = att['data']
                is_inline = att.get('is_inline', False)
                att_content_type = att.get('content_type', '')
                
                # Log inline images separately
                if is_inline and att_content_type.startswith('image/'):
                    log(f"[INFO] Found inline image: {filename}")

            if not filename or not att_data:
                continue

            file_path = f"{email_dir}/{filename}"
            file_extension_att = filename.split('.')[-1].lower() if '.' in filename else ''
            
            # Check if this is a nested email file (.msg or .eml) with raw bytes
            if file_extension_att in ['msg', 'eml'] and isinstance(att_data, bytes):
                log(f"[INFO] Found nested email file: {filename}")
                
                upload_to_supabase(BUCKET_NAME, file_path, att_data)
                public_url = supabase_read.storage.from_(BUCKET_NAME).get_public_url(file_path).rstrip('?')
                
                attachment_data_list.append({
                    'file_name': filename,
                    'file_path': file_path,
                    'file_size_bytes': len(att_data),
                    'file_extension': file_extension_att,
                    'attachment_index': idx,
                    'attachment_processing': f"Nested email file: {filename}",
                    'attachment_validity': None,
                    'file_url': public_url
                })
                
                nested_attachments = process_nested_email(
                    att_data, 
                    filename, 
                    email_dir, 
                    None,
                    idx
                )
                
                attachment_data_list.extend(nested_attachments)
                
            else:
                # Regular attachment processing
                log(f"[INFO] Uploading attachment {filename} to {file_path}")
                upload_to_supabase(BUCKET_NAME, file_path, att_data)

                attachment_paths.append(file_path)
                public_url = supabase_read.storage.from_(BUCKET_NAME).get_public_url(file_path).rstrip('?')

                excel_content = None
                if file_extension_att in ['xlsx', 'xls', 'xlsm', 'xlsb']:
                    log(f"[INFO] Extracting Excel content from {filename}")
                    excel_content = extract_excel_content(att_data, filename)
                
                attachment_data_list.append({
                    'file_name': filename,
                    'file_path': file_path,
                    'file_size_bytes': len(att_data) if att_data else 0,
                    'file_extension': file_extension_att,
                    'attachment_index': idx,
                    'attachment_processing': excel_content,
                    'attachment_validity': None,
                    'file_url': public_url
                })

        # Close MSG file if it was opened
        if file_extension == 'msg':
            msg.close()

        # Parse EML date if it's a string
        if file_extension == 'eml' and isinstance(msg_date, str):
            try:
                from email.utils import parsedate_to_datetime
                msg_date = parsedate_to_datetime(msg_date)
            except:
                msg_date = None

        # Calculate processing duration
        duration_ms = int((time.time() - start_time) * 1000)
        processing_completed_at = datetime.now().isoformat()

        # Prepare email data for database
        email_data = {
            'folder': folder_name,
            'file_name': email_name,
            'file_size_bytes': len(email_bytes),
            'sender': msg_sender,
            'recipients': msg_to,
            'cc_recipients': msg_cc,
            'bcc_recipients': msg_bcc,
            'email_date': msg_date.isoformat() if msg_date else None,
            'subject': msg_subject,
            'body_preview': msg_body[:200] if msg_body else '',
            'body_file_path': txt_path,
            'attachment_count': len(attachment_data_list),
            'processing_status': 'success',
            'processing_duration_ms': duration_ms,
            'processing_started_at': processing_started_at,
            'processing_completed_at': processing_completed_at
        }

        email_id = insert_email_to_db(email_data)

        if email_id and attachment_data_list:
            for att_data in attachment_data_list:
                att_data['email_id'] = email_id
            insert_attachments_to_db(email_id, attachment_data_list)

        legacy_record = {
            "Folder": folder_name,
            "FileName": email_name,
            "Sender": msg_sender,
            "Date": msg_date,
            "Subject": msg_subject,
            "BodyFile": txt_path,
            "Attachments": ";".join(attachment_paths)
        }
        records.append(legacy_record)

        newly_processed_emails.add(email_base_name) 

        return email_id

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        processing_completed_at = datetime.now().isoformat()

        error_data = {
            'folder': folder_name,
            'file_name': email_name,
            'file_size_bytes': len(email_bytes) if email_bytes else 0,
            'processing_status': 'failed',
            'error_message': str(e),
            'processing_duration_ms': duration_ms,
            'processing_started_at': processing_started_at,
            'processing_completed_at': processing_completed_at
        }

        try:
            insert_email_to_db(error_data)
        except Exception as db_error:
            log(f"[ERROR] Failed to log error to database: {db_error}")

        log(f"[ERROR] Failed to process '{email_name}': {e}")
        return None

def list_all_files(bucket, path=""):
    """List all .msg files in Supabase storage using anon key"""
    all_files = []
    log(f"[DEBUG] Listing contents of: '{path}'")
    try:
        items = supabase_read.storage.from_(bucket).list(path=path)
        log(f"[DEBUG] Found {len(items)} items in '{path}'")
        for item in items:
            name = item["name"]
            item_id = item.get("id")
            full_path = f"{path}/{name}".replace("//", "/") if path else name
            if item_id is None:
                all_files.extend(list_all_files(bucket, full_path))
            elif name.lower().endswith((".msg", ".eml")):
                all_files.append(full_path)
    except Exception as e:
        log(f"[ERROR] Failed to list '{path}': {e}")
    return all_files

def process_all_emails():
    global processing_complete, newly_processed_emails
    newly_processed_emails = set()
    log("[INFO] Starting email processing...")

    try:
        # --- STEP 1: Get already processed files from database ---
        processed_files = get_processed_files()

        # --- STEP 2: Get list of .msg files from storage ---
        all_files = list_all_files(BUCKET_NAME, path=INPUT_FOLDER)
        log(f"[INFO] Total email files (.msg and .eml) found: {len(all_files)}")

        processed_count = 0
        skipped_count = 0
        failed_count = 0

        # --- STEP 3: Process each email file ---
        for file_path in all_files:
            try:
                file_name = os.path.basename(file_path)
                folder_name = os.path.dirname(file_path).replace(INPUT_FOLDER, "").strip("/\\") or "root"

                # Skip if already processed
                if (file_name, folder_name) in processed_files:
                    log(f"[SKIP] '{file_name}' already processed. Skipping.")
                    skipped_count += 1
                    continue

                log(f"[INFO] Downloading {file_path}...")
                msg_bytes = supabase_read.storage.from_(BUCKET_NAME).download(file_path)
                if not msg_bytes:
                    log(f"[WARNING] File {file_path} is empty")
                    continue

                log(f"[INFO] Processing '{file_name}' from folder '{folder_name}'")
                email_id = process_email_file(msg_bytes, file_name, folder_name)
                
                if email_id:
                    processed_count += 1
                    newly_processed_emails.add(file_name)  # ✅ Track new emails
                    log(f"[NEW] Successfully processed and inserted: {file_name}")
                elif email_id is None:
                    # This means it was a duplicate caught by the database
                    skipped_count += 1
                    log(f"[SKIP] Duplicate email not inserted: {file_name}")
                else:
                    failed_count += 1

            except Exception as e:
                log(f"[ERROR] Failed to download or process {file_path}: {e}")
                failed_count += 1

        # --- STEP 4: Log summary ---
        log(f"[INFO] ✅ Processing complete!")
        log(f"[INFO] Summary: {processed_count} processed, {skipped_count} skipped, {failed_count} failed")

        # Get total count from database using read client
        try:
            total_result = supabase_read.table('emails').select('id', count='exact').execute()
            total_count = total_result.count if hasattr(total_result, 'count') else 'unknown'
            log(f"[INFO] Total emails in database: {total_count}")
        except Exception as e:
            log(f"[WARNING] Could not get total count from database: {e}")

    except Exception as e:
        log(f"[ERROR] Fatal error during processing: {e}")
    finally:
        processing_complete = True

# --- N8N INTEGRATION ---
N8N_WEBHOOK_URL = "http://localhost:5678/webhook/3fed9fcb-efb2-42a3-a8a5-67bb41971edd"

# Track progress per folder
n8n_status = {}  # {folder_name: {"status": "pending/sent/completed", "urls": [...]}}

def get_processed_emails_from_db():
    """
    Query database for emails that have already been sent to n8n
    Returns a set of file_names that should be skipped
    """
    try:
        # Get all emails from database
        result = supabase_read.table('emails').select('file_name').execute()
        processed_emails = {r['file_name'] for r in result.data}
        log(f"[DB] Found {len(processed_emails)} emails in database")
        return processed_emails
    except Exception as e:
        log(f"[ERROR] Failed to query emails from database: {e}")
        return set()


def extract_filename_from_path(file_path):
    """
    Extract the original .msg filename from the attachment path
    Example: email_attachments/Check/hiv/attachment.pdf -> msg or eml
    """
    # Split path and get the email folder name (last component of the directory)
    parts = file_path.split('/')
    if len(parts) >= 3:
        # For path like 'email_attachments/Check/hiv', we want 'hiv'
        # parts[-1] is the last folder name (the email name without .msg)
        email_name = parts[-1]  # Changed from parts[-2] to parts[-1]
        return email_name
    return None


def list_attachments_by_folder(bucket, base_folder="email_attachments"):
    """
    Returns a dict of folders -> list of attachment URLs
    """
    attachments_by_folder = {}
    try:
        top_level_items = supabase_read.storage.from_(bucket).list(path=base_folder)
        for folder_item in top_level_items:
            folder_name = folder_item['name']
            folder_path = f"{base_folder}/{folder_name}"
            sub_items = supabase_read.storage.from_(bucket).list(path=folder_path)
            for email_sub in sub_items:
                email_folder = f"{folder_path}/{email_sub['name']}"
                email_attachments = supabase_read.storage.from_(bucket).list(path=email_folder)
                urls = []
                for att in email_attachments:
                    url = f"{email_folder}/{att['name']}"
                    urls.append(url)
                if urls:
                    attachments_by_folder[email_folder] = urls
    except Exception as e:
        log(f"[ERROR] Failed to list attachments by folder: {e}")
    return attachments_by_folder


def update_email_n8n_url(email_filename, n8n_url):
    """Update the n8n_url field in the emails table"""
    try:
        # First, try exact match
        result = supabase_write.table('emails').update({
            'n8n_url': n8n_url
        }).eq('file_name', email_filename).execute()
        
        if result.data:
            log(f"[DB] Updated n8n_url for email '{email_filename}'")
            return True
        
        # If no exact match, try case-insensitive search and also try alternate extension
        log(f"[DEBUG] Exact match failed, trying case-insensitive search for '{email_filename}'")
        
        # Get all emails and find case-insensitive match
        all_emails = supabase_write.table('emails').select('file_name').execute()
        matching_filename = None
        
        # Try matching with or without extension variations
        base_name = email_filename.replace('.msg', '').replace('.eml', '')
        
        for email in all_emails.data:
            email_base = email['file_name'].replace('.msg', '').replace('.eml', '')
            if email_base.lower() == base_name.lower():
                matching_filename = email['file_name']
                break
        
        if matching_filename:
            result = supabase_write.table('emails').update({
                'n8n_url': n8n_url
            }).eq('file_name', matching_filename).execute()
            
            if result.data:
                log(f"[DB] Updated n8n_url for email '{matching_filename}' (matched '{email_filename}')")
                return True
        
        log(f"[WARN] No email found with file_name '{email_filename}' to update (case-insensitive)")
        return False
        
    except Exception as e:
        log(f"[ERROR] Failed to update n8n_url for '{email_filename}': {e}")
        return False


def send_all_attachments_to_n8n():
    """
    Send attachment URLs to n8n ONLY for newly processed emails in this run.
    """
    global n8n_processing_complete
    log("[INFO] Starting n8n attachment sending for newly processed emails...")

    if not newly_processed_emails:
        log("[INFO] No new emails to send to n8n. Skipping.")
        n8n_processing_complete = True
        return

    attachments_by_folder = list_attachments_by_folder(BUCKET_NAME)
    if not attachments_by_folder:
        log("[WARN] No attachments found to send to n8n.")
        n8n_processing_complete = True
        return

    sent_count = 0
    skipped_count = 0

    for folder, urls in attachments_by_folder.items():
        email_folder_name = extract_filename_from_path(folder)

        # CRITICAL: Check if email is newly processed - match without extension
        is_newly_processed = False
        for processed in newly_processed_emails:
            # Remove extension from both for comparison
            processed_name = processed.replace('.msg', '').replace('.eml', '')
            if email_folder_name and email_folder_name.lower() == processed_name.lower():
                is_newly_processed = True
                email_filename = processed  # Use the actual filename with extension
                break
        
        if not is_newly_processed:
            log(f"[SKIP] Email folder '{email_folder_name}' not newly processed in this run. Skipping n8n send.")
            skipped_count += len(urls)
            continue
        
        # If we get here, this email WAS newly processed, so send ALL its attachments
        log(f"[INFO] Processing folder '{folder}' ({len(urls)} attachments) - NEWLY PROCESSED EMAIL")

        n8n_status[folder] = {"status": "pending", "urls": urls, "completed": []}
        folder_outcomes = []  # ✅ This will store results in order

        # ✅ Step 1: Process .txt files FIRST and extract their content
        txt_files = []
        other_files = []
        
        for url in urls:
            if url.endswith('.txt') and not url.endswith('n8n_results.txt'):
                txt_files.append(url)
            elif not url.endswith('n8n_results.txt'):
                other_files.append(url)
        
        # ✅ Record .txt files FIRST with actual content
        for url in txt_files:
            try:
                # Download the email body text file from Supabase
                log(f"[INFO] Downloading email body content from: {url}")
                email_body_content = supabase_read.storage.from_(BUCKET_NAME).download(url)
                
                if email_body_content:
                    # Decode bytes to string
                    email_body_text = email_body_content.decode('utf-8', errors='ignore')
                    
                    # Add separator and actual email body content to results
                    folder_outcomes.append(f"=" * 80)
                    folder_outcomes.append(f"EMAIL BODY CONTENT FROM: {url}")
                    folder_outcomes.append(f"=" * 80)
                    folder_outcomes.append(email_body_text)
                    folder_outcomes.append(f"=" * 80)
                    folder_outcomes.append("")  # Empty line for spacing
                    
                    n8n_status[folder]["completed"].append({"url": url, "outcome": "email_body_extracted"})
                    log(f"[INFO] Extracted and stored email body content ({len(email_body_text)} chars)")
                else:
                    folder_outcomes.append(f"{url} -> error: could not download email body")
                    log(f"[WARN] Could not download email body from: {url}")
                    
            except Exception as e:
                folder_outcomes.append(f"{url} -> error: {str(e)}")
                log(f"[ERROR] Failed to extract email body from {url}: {e}")

        # ✅ Step 2: Send other attachments to n8n and APPEND to folder_outcomes
        for url in other_files:
            try:
                log(f"[INFO] Sending attachment to n8n: {url}")
                response = requests.post(N8N_WEBHOOK_URL, json={"attachment_url": url}, timeout=120)
                log(f"[DEBUG] n8n response status: {response.status_code}")
                log(f"[DEBUG] n8n response body: {response.text[:200]}")
                try:
                    outcome = response.json()
                except ValueError:
                    outcome = response.text
                sent_count += 1
            except Exception as e:
                log(f"[ERROR] Failed to send attachment {url} to n8n: {e}")
                outcome = f"error_exception: {str(e)}"

            folder_outcomes.append(f"{url} -> {str(outcome)}")
            n8n_status[folder]["completed"].append({"url": url, "outcome": outcome})

        # Step 3: Save results for the folder
        result_text = "\n".join(folder_outcomes)
        file_path = f"{folder}/n8n_results.txt"
        try:
            upload_to_supabase(BUCKET_NAME, file_path, result_text, content_type="text/plain")
            update_email_n8n_url(email_filename, file_path)

            n8n_status[folder]["status"] = "completed"
            log(f"[INFO] ✅ Results for n8n.txt saved and linked in DB.")
            
            # ✅ Step 4: Send the n8n_results.txt URL back to n8n
            log(f"[INFO] Sending n8n_results_url to n8n: {file_path}")
            try:
                response = requests.post(N8N_WEBHOOK_URL, json={
                    "attachment_url": file_path,
                    "is_result_file": True,
                    "email_filename": email_filename
                }, timeout=120)
                try:
                    result_outcome = response.json()
                except ValueError:
                    result_outcome = response.text
                log(f"[INFO] ✅ n8n_results_url sent successfully")
                sent_count += 1
            except Exception as e:
                log(f"[ERROR] Failed to send n8n_results_url to n8n: {e}")
            
        except Exception as e:
            log(f"[ERROR] Failed to save n8n results for '{email_filename}': {e}")
            n8n_status[folder]["status"] = "error"

    log(f"[INFO] n8n sending complete. Sent: {sent_count}, Skipped: {skipped_count}")
    n8n_processing_complete = True


def process_all_emails_with_n8n():
    """Combined function to process emails and send to n8n"""
    global newly_processed_emails
    newly_processed_emails = set()  # Clear before starting
    process_all_emails()
    send_all_attachments_to_n8n()
    newly_processed_emails = set()  # Clear after completion


# --- FLASK APP ---
app = Flask(__name__)

@app.route("/")
def index():
    return render_template_string("""
    <html>
        <head>
            <title>BestMed Email Processing Dashboard - Complete Management</title>
            <style>
                body { font-family: Arial, sans-serif; background: #1e1e1e; color: #d4d4d4; margin: 20px; }
                h1 { color: #4ec9b0; text-align: center;}
                .control-panel { margin: 20px 0; display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
                .search-bar { 
                    padding: 8px; 
                    border: 1px solid #333; 
                    background: #2d2d30; 
                    color: #d4d4d4; 
                    border-radius: 4px;
                    width: 300px;
                }
                button { 
                    background: #4ec9b0; 
                    color: black; 
                    padding: 10px 20px; 
                    border: none; 
                    border-radius: 5px; 
                    cursor: pointer; 
                    font-size: 14px;
                    font-weight: bold;
                }
                button:hover { background: #3ba892; }
                button:disabled { background: #666; cursor: not-allowed; }
                .download-btn { background: #ff6b6b; }
                .download-btn:hover { background: #ff5252; }
                .process-btn { background: #ffa500; }
                .process-btn:hover { background: #ff8c00; }
                #status-message { 
                    padding: 15px; 
                    margin: 10px 0; 
                    border-radius: 8px; 
                    background: #2d2d30;
                    border-left: 4px solid #4ec9b0;
                }
                .stats-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                    gap: 15px;
                    margin: 20px 0;
                }
                .stat-box {
                    background: #2d2d30;
                    padding: 15px;
                    border-radius: 8px;
                    text-align: center;
                }
                .stat-number { font-size: 24px; font-weight: bold; color: #4ec9b0; }
                .stat-label { font-size: 12px; color: #9cdcfe; }
                table { width: 100%; border-collapse: collapse; margin-top: 20px; }
                th, td { padding: 8px; text-align: left; border-bottom: 1px solid #333; font-size: 13px; }
                th { background: #333; color: #9cdcfe; position: sticky; top: 0; }
                tr:hover { background: #2d2d30; }
                .success { color: #4ec9b0; font-weight: bold; }
                .failed { color: #f14c4c; font-weight: bold; }
                .pending { color: #cca700; font-weight: bold; }
                .complete { color: #4ec9b0; font-weight: bold; }
                .incomplete { color: #f14c4c; font-weight: bold; }
                .complete-row { background: #2d4a2d; }
                .event-id { font-family: monospace; color: #9cdcfe; font-size: 11px; }
                .tabs {
                    display: flex;
                    background: #333;
                    border-radius: 8px 8px 0 0;
                    margin-top: 20px;
                }
                .tab {
                    padding: 12px 24px;
                    cursor: pointer;
                    background: #333;
                    color: #9cdcfe;
                    border: none;
                    border-radius: 8px 8px 0 0;
                    flex: 1;
                    text-align: center;
                }
                .tab.active { background: #4ec9b0; color: black; }
                .tab-content { display: none; }
                .tab-content.active { display: block; }
                .table-container {
                    max-height: 500px;
                    overflow-y: auto;
                    border: 1px solid #333;
                    border-radius: 0 0 8px 8px;
                }
                .filter-pills {
                    display: flex;
                    gap: 5px;
                    margin: 10px 0;
                    flex-wrap: wrap;
                }
                .filter-pill {
                    padding: 4px 8px;
                    background: #333;
                    border: none;
                    border-radius: 12px;
                    color: #d4d4d4;
                    font-size: 12px;
                    cursor: pointer;
                }
                .filter-pill.active { background: #4ec9b0; color: black; }
            </style>
        </head>
        <body>
            <h1> BestMed Email Processing Dashboard - Complete Management</h1>
            
            <div class="stats-grid" id="stats-grid">
                <div class="stat-box">
                    <div class="stat-number" id="total-emails">0</div>
                    <div class="stat-label">Total Emails</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number" id="total-events">0</div>
                    <div class="stat-label">Total Events</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number" id="processed-emails">0</div>
                    <div class="stat-label">Successfully Processed</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number" id="with-json">0</div>
                    <div class="stat-label">With JSON Payload</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number" id="complete-payloads">0</div>
                    <div class="stat-label">Complete w/ Binary</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number" id="completion-rate">0%</div>
                    <div class="stat-label">Completion Rate</div>
                </div>
            </div>
            
            <div class="control-panel">
                <button id="start-btn" onclick="startProcessing()">▶️ Start Processing</button>
                <button id="refresh-btn" onclick="loadAllData()">🔄 Refresh All</button>
                <button class="process-btn" onclick="processAllPayloads()">⚙️ Complete JSON Payloads</button>
                <button class="download-btn" onclick="downloadCompletePayloads()">📦 Download Complete Payloads</button>
                <button class="process-btn" onclick="validateAll()">✅ Validate All Classifications</button>
                <input type="text" class="search-bar" id="search-input" 
                       placeholder="Search Event ID, file name, sender..." 
                       oninput="filterCurrentTable()">
            </div>
            
            <div id="status-message">Status: Ready</div>

            <div class="tabs">
                <button class="tab active" onclick="showTab('processing')">📧 Email Processing Status</button>
                <button class="tab" onclick="showTab('events')">🔖 Event IDs</button>
                <button class="tab" onclick="showTab('json-completion')">📄 JSON Payload Completion</button>
            </div>

            <!-- Email Processing Tab -->
            <div id="processing-tab" class="tab-content active">
                <div class="filter-pills">
                    <button class="filter-pill active" onclick="filterByProcessingStatus('all')">All</button>
                    <button class="filter-pill" onclick="filterByProcessingStatus('success')">Success</button>
                    <button class="filter-pill" onclick="filterByProcessingStatus('failed')">Failed</button>
                </div>
                
                <div class="table-container">
                    <table id="processing-table">
                        <thead>
                            <tr>
                                <th>Event ID</th>
                                <th>File Name</th>
                                <th>Processing Status</th>
                                <th>Original Classification</th>
                                <th>Confidence Score</th>
                                <th>Final Classification</th>
                                <th>JSON Payload Status</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>

            <!-- Event IDs Tab -->
            <div id="events-tab" class="tab-content">
                <div class="filter-pills">
                    <button class="filter-pill active" onclick="filterByPayloadStatus('all')">All</button>
                    <button class="filter-pill" onclick="filterByPayloadStatus('pending')">Pending</button>
                    <button class="filter-pill" onclick="filterByPayloadStatus('validated')">Validated</button>
                    <button class="filter-pill" onclick="filterByPayloadStatus('failed')">Failed</button>
                </div>
                
                <div class="table-container">
                    <table id="events-table">
                        <thead>
                            <tr>
                                <th>Event ID</th>
                                <th>Email File</th>
                                <th>Sender</th>
                                <th>Subject</th>
                                <th>Payload Status</th>
                                <th>Created At</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>

            <!-- JSON Completion Tab -->
            <div id="json-completion-tab" class="tab-content">
                <div class="filter-pills">
                    <button class="filter-pill active" onclick="filterByCompletionStatus('all')">All</button>
                    <button class="filter-pill" onclick="filterByCompletionStatus('complete')">Complete</button>
                    <button class="filter-pill" onclick="filterByCompletionStatus('incomplete')">Missing Binary</button>
                </div>
                
                <div class="table-container">
                    <table id="completion-table">
                        <thead>
                            <tr>
                                <th>Event ID</th>
                                <th>Email File</th>
                                <th>Document Type</th>
                                <th>Member ID</th>
                                <th>File Binary Status</th>
                                <th>Binary Size</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
            
            <script>
                let currentInterval = null;
                let allProcessingData = [];
                let allEventsData = [];
                let allCompletionData = [];
                let currentTab = 'processing';
                let currentProcessingFilter = 'all';
                let currentPayloadFilter = 'all';
                let currentCompletionFilter = 'all';
                
                function showTab(tabName) {
                    document.querySelectorAll('.tab-content').forEach(tab => {
                        tab.classList.remove('active');
                    });
                    document.querySelectorAll('.tab').forEach(tab => {
                        tab.classList.remove('active');
                    });
                    
                    document.getElementById(tabName + '-tab').classList.add('active');
                    event.target.classList.add('active');
                    currentTab = tabName;
                    
                    if (tabName === 'json-completion') {
                        loadCompletionData();
                    } else if (tabName === 'events') {
                        loadEventsData();
                    } else {
                        loadProcessingData();
                    }
                }
                
                async function startProcessing() {
                    const btn = document.getElementById('start-btn');
                    const status = document.getElementById('status-message');
                    
                    try {
                        const healthCheck = await fetch('/health');
                        const healthData = await healthCheck.json();
                        
                        if (healthData.processing_started && !healthData.processing_complete) {
                            status.textContent = 'Status: Processing already in progress...';
                            return;
                        }
                        
                        btn.disabled = true;
                        status.textContent = 'Status: Starting processing...';
                        
                        const res = await fetch('/start-processing');
                        const data = await res.json();
                        status.textContent = 'Status: ' + data.message;
                        
                        if (currentInterval) {
                            clearInterval(currentInterval);
                        }
                        
                        currentInterval = setInterval(async () => {
                            await loadAllData();
                            const health = await fetch('/health');
                            const healthData = await health.json();
                            
                            if (healthData.processing_complete && healthData.n8n_processing_complete) {
                                clearInterval(currentInterval);
                                currentInterval = null;
                                btn.disabled = false;
                                status.textContent = 'Status: ✅ All processing complete! Ready for JSON completion.';
                            } else {
                                status.textContent = `Status: Processing... (${healthData.emails_success} success, ${healthData.emails_failed} failed)`;
                            }
                        }, 5000);
                    } catch (error) {
                        status.textContent = 'Status: Error starting processing';
                        btn.disabled = false;
                    }
                }
                
                async function loadProcessingData() {
                    try {
                        const res = await fetch('/emails');
                        const data = await res.json();
                        
                        if (data.error) {
                            document.querySelector('#processing-table tbody').innerHTML = 
                                '<tr><td colspan="8">Error loading emails</td></tr>';
                            return;
                        }
                        
                        allProcessingData = data;
                        updateProcessingStats(data);
                        filterProcessingTable();
                        
                    } catch (error) {
                        document.getElementById('status-message').textContent = 'Error loading processing data';
                    }
                }
                
                async function loadEventsData() {
                    try {
                        const res = await fetch('/events');
                        const data = await res.json();
                        
                        if (data.error) {
                            document.querySelector('#events-table tbody').innerHTML = 
                                '<tr><td colspan="7">Error loading events</td></tr>';
                            return;
                        }
                        
                        allEventsData = data;
                        filterEventsTable();
                        
                    } catch (error) {
                        document.getElementById('status-message').textContent = 'Error loading events data';
                    }
                }
                
                async function loadCompletionData() {
                    try {
                        const res = await fetch('/json-payload-status');
                        const data = await res.json();
                        
                        if (data.status === 'error') {
                            document.querySelector('#completion-table tbody').innerHTML = 
                                '<tr><td colspan="7">Error loading completion data</td></tr>';
                            return;
                        }
                        
                        allCompletionData = data.details || [];
                        updateCompletionStats(data);
                        filterCompletionTable();
                        
                    } catch (error) {
                        document.getElementById('status-message').textContent = 'Error loading completion data';
                    }
                }
            
                            
                async function loadAllData() {
                    await Promise.all([loadProcessingData(), loadEventsData(), loadCompletionData()]);
                }
                                  
                async function validateAll() {
                    const statusDiv = document.getElementById('status-message');
                    statusDiv.textContent = 'Status: Validating all classifications...';
    
                    try {
                        const res = await fetch('/validate-all');
                        const data = await res.json();
        
                        if (data.status === 'success') {
                            statusDiv.textContent = `Status: ✅ Validation complete! Valid: ${data.valid}, Unindexed: ${data.invalid}, Errors: ${data.errors}`;
                            setTimeout(() => loadAllData(), 2000);
                        } else {
                            statusDiv.textContent = 'Status: ❌ ' + data.message;
                        }
                    } catch (error) {
                        statusDiv.textContent = 'Status: Error validating';
                    }
                }
                
                function updateProcessingStats(data) {
                    const totalEmails = data.length;
                    const processedEmails = data.filter(e => e.processing_status === 'success').length;
                    const withJson = data.filter(e => e.json_payload).length;
                    const withEventIds = data.filter(e => e.event_id).length;
                    
                    document.getElementById('total-emails').textContent = totalEmails;
                    document.getElementById('total-events').textContent = withEventIds;
                    document.getElementById('processed-emails').textContent = processedEmails;
                    document.getElementById('with-json').textContent = withJson;
                }
                
                function updateCompletionStats(data) {
                    const completionRate = data.total_with_json > 0 ? 
                        Math.round((data.complete_with_binary / data.total_with_json) * 100) : 0;
                    
                    document.getElementById('complete-payloads').textContent = data.complete_with_binary;
                    document.getElementById('completion-rate').textContent = completionRate + '%';
                }
                
                function filterCurrentTable() {
                    if (currentTab === 'processing') {
                        filterProcessingTable();
                    } else if (currentTab === 'events') {
                        filterEventsTable();
                    } else {
                        filterCompletionTable();
                    }
                }
                
                function filterProcessingTable() {
                    const searchTerm = document.getElementById('search-input').value.toLowerCase();
                    let filtered = allProcessingData;
                    
                    if (currentProcessingFilter !== 'all') {
                        filtered = filtered.filter(email => email.processing_status === currentProcessingFilter);
                    }
                    
                    if (searchTerm) {
                        filtered = filtered.filter(email => 
                            (email.event_id || '').toLowerCase().includes(searchTerm) ||
                            (email.file_name || '').toLowerCase().includes(searchTerm) ||
                            (email.sender || '').toLowerCase().includes(searchTerm)
                        );
                    }
                    
                    renderProcessingTable(filtered);
                }
                
                function filterEventsTable() {
                    const searchTerm = document.getElementById('search-input').value.toLowerCase();
                    let filtered = allEventsData;
                    
                    if (currentPayloadFilter !== 'all') {
                        filtered = filtered.filter(email => email.payload_status === currentPayloadFilter);
                    }
                    
                    if (searchTerm) {
                        filtered = filtered.filter(email => 
                            (email.event_id || '').toLowerCase().includes(searchTerm) ||
                            (email.file_name || '').toLowerCase().includes(searchTerm) ||
                            (email.sender || '').toLowerCase().includes(searchTerm)
                        );
                    }
                    
                    renderEventsTable(filtered);
                }
                
                function filterCompletionTable() {
                    const searchTerm = document.getElementById('search-input').value.toLowerCase();
                    let filtered = allCompletionData;
                    
                    if (currentCompletionFilter === 'complete') {
                        filtered = filtered.filter(email => email.has_file_binary);
                    } else if (currentCompletionFilter === 'incomplete') {
                        filtered = filtered.filter(email => !email.has_file_binary);
                    }
                    
                    if (searchTerm) {
                        filtered = filtered.filter(email => 
                            (email.event_id || '').toLowerCase().includes(searchTerm) ||
                            (email.file_name || '').toLowerCase().includes(searchTerm) ||
                            (email.member || '').toLowerCase().includes(searchTerm) ||
                            (email.document_type || '').toLowerCase().includes(searchTerm)
                        );
                    }
                    
                    renderCompletionTable(filtered);
                }
                
                function renderProcessingTable(data) {
                    const tbody = document.querySelector('#processing-table tbody');
                    tbody.innerHTML = '';
                    
                    data.forEach(row => {
                        const tr = document.createElement('tr');
                        const statusClass = row.processing_status === 'success' ? 'success' :
                                            row.processing_status === 'failed' ? 'failed' : 'pending';
                        
                        // Use payload_status to determine JSON payload status
                        const payloadStatus = row.payload_status || 'pending';
                        let jsonPayloadDisplay = '';
                        let jsonPayloadClass = '';
                        
                        if (!row.json_payload) {
                            jsonPayloadDisplay = '❌ Missing';
                            jsonPayloadClass = 'failed';
                        } else if (payloadStatus === 'validated') {
                            jsonPayloadDisplay = '✅ Validated';
                            jsonPayloadClass = 'success';
                        } else if (payloadStatus === 'failed') {
                            jsonPayloadDisplay = '❌ Failed';
                            jsonPayloadClass = 'failed';
                        } else {
                            jsonPayloadDisplay = '⏳ Pending';
                            jsonPayloadClass = 'pending';
                        }
                        
                        // Determine if View button should be enabled
                        const canView = row.json_payload && payloadStatus === 'validated';
                        const viewButtonDisabled = !canView ? 'disabled style="opacity: 0.5; cursor: not-allowed;"' : '';
                                  

                        
                        tr.innerHTML = `
                            <td><span class="event-id">${row.event_id || 'N/A'}</span></td>
                            <td>${row.file_name}</td>
                            <td class="${statusClass}">${row.processing_status}</td>
                            <td>${row.classification_check || '-'}</td>
                            <td>${row.confidence_score || '-'}</td>
                            <td class="${row.final_classification === 'UNINDEXED WORKFLOWS' ? 'failed' : 'success'}">${row.final_classification || '-'}</td>
                            <td class="${jsonPayloadClass}">${jsonPayloadDisplay}</td>
                            <td>
                                ${row.json_payload ? 
                                    `<button onclick="viewPayload('${encodeURIComponent(row.file_name.replace('.msg', ''))}')" ${viewButtonDisabled} style="padding: 4px 8px; font-size: 12px;">👁️ View</button>` : 
                                    '<span style="color: #666;">-</span>'}
                            </td>
                        `;
                        tbody.appendChild(tr);
                    });
                }
                
                function renderEventsTable(data) {
                    const tbody = document.querySelector('#events-table tbody');
                    tbody.innerHTML = '';
                    
                    data.forEach(row => {
                        const tr = document.createElement('tr');
                        const payloadStatus = row.payload_status || 'pending';
                        const payloadClass = payloadStatus === 'validated' ? 'success' :
                                        payloadStatus === 'failed' ? 'failed' : 'pending';
                        
                        // Determine if View button should be enabled
                        const canView = payloadStatus !== 'pending';
                        const viewButtonDisabled = !canView ? 'disabled style="opacity: 0.5; cursor: not-allowed;"' : '';
                        
                        tr.innerHTML = `
                            <td><span class="event-id">${row.event_id || 'N/A'}</span></td>
                            <td>${row.file_name}</td>
                            <td>${row.sender || 'Unknown'}</td>
                            <td>${row.subject || 'No Subject'}</td>
                            <td class="${payloadClass}">${payloadStatus}</td>
                            <td>${row.created_at ? new Date(row.created_at).toLocaleString() : 'N/A'}</td>
                            <td>
                                <button onclick="viewPayload('${encodeURIComponent(row.file_name.replace('.msg', ''))}')" ${viewButtonDisabled} style="padding: 4px 8px; font-size: 12px;">👁️ View</button>
                            </td>
                        `;
                        tbody.appendChild(tr);
                    });
                }
                
                function renderCompletionTable(data) {
                    const tbody = document.querySelector('#completion-table tbody');
                    tbody.innerHTML = '';
                    
                    data.forEach(row => {
                        const tr = document.createElement('tr');
                        const statusClass = row.has_file_binary ? 'complete' : 'incomplete';
                        const sizeKB = row.file_binary_size > 0 ? (row.file_binary_size / 1024).toFixed(1) + ' KB' : '0 KB';
                        
                        if (row.has_file_binary) {
                            tr.classList.add('complete-row');
                        }
                        
                        tr.innerHTML = `
                            <td><span class="event-id">${row.event_id || 'N/A'}</span></td>
                            <td>${row.file_name}</td>
                            <td>${row.document_type}</td>
                            <td>${row.member}</td>
                            <td class="${statusClass}">${row.has_file_binary ? '✅ Complete' : '❌ Missing'}</td>
                            <td>${sizeKB}</td>
                            <td>
                                ${!row.has_file_binary ? 
                                    `<button onclick="processSingleEmail('${row.file_name.replace('.msg', '')}')" style="padding: 4px 8px; font-size: 12px;">🔧 Process</button>` : 
                                    `<button onclick="viewPayload('${encodeURIComponent(row.file_name.replace('.msg', ''))}')" style="padding: 4px 8px; font-size: 12px;">👁️ View</button>`
                                }
                            </td>
                        `;
                        tbody.appendChild(tr);
                    });
                }
                
                function filterByProcessingStatus(status) {
                    currentProcessingFilter = status;
                    document.querySelectorAll('#processing-tab .filter-pill').forEach(pill => {
                        pill.classList.remove('active');
                    });
                    event.target.classList.add('active');
                    filterProcessingTable();
                }
                
                function filterByPayloadStatus(status) {
                    currentPayloadFilter = status;
                    document.querySelectorAll('#events-tab .filter-pill').forEach(pill => {
                        pill.classList.remove('active');
                    });
                    event.target.classList.add('active');
                    filterEventsTable();
                }
                
                function filterByCompletionStatus(status) {
                    currentCompletionFilter = status;
                    document.querySelectorAll('#json-completion-tab .filter-pill').forEach(pill => {
                        pill.classList.remove('active');
                    });
                    event.target.classList.add('active');
                    filterCompletionTable();
                }
                
                async function processAllPayloads() {
                    const statusDiv = document.getElementById('status-message');
                    statusDiv.textContent = 'Status: Processing all JSON payloads...';
                    
                    try {
                        const res = await fetch('/process-json-payloads');
                        const data = await res.json();
                        
                        if (data.status === 'success') {
                            statusDiv.textContent = `Status: ✅ ${data.message}`;
                            setTimeout(() => loadAllData(), 2000);
                        } else {
                            statusDiv.textContent = 'Status: ❌ ' + data.message;
                        }
                    } catch (error) {
                        statusDiv.textContent = 'Status: Error processing payloads';
                    }
                }
                
                async function processSingleEmail(emailName) {
                    try {
                        const res = await fetch(`/process-single-email/${emailName}`);
                        const data = await res.json();
                        
                        if (data.status === 'success') {
                            alert(`✅ Successfully processed ${emailName}`);
                            loadAllData();
                        } else {
                            alert(`❌ Failed to process ${emailName}: ${data.message}`);
                        }
                    } catch (error) {
                        alert('Error processing email');
                    }
                }
                
               async function viewPayload(emailName) {
                    const clickedButton = event.target;
                    if (clickedButton.disabled) {
                        alert('⏳ This payload is still being processed or validation failed. Please wait until validation is complete or check the status.');
                        return;
                    }

                    try {
                        const encodedName = encodeURIComponent(emailName);
                        const res = await fetch(`/json-payload/${encodedName}`);
                        
                        if (!res.ok) {
                            throw new Error(`HTTP ${res.status}: ${res.statusText}`);
                        }
                        
                        const payload = await res.json();
                        
                        const newWindow = window.open('', '_blank', 'width=900,height=700');
                        newWindow.document.write(`
                            <html>
                                <head>
                                    <title>Complete JSON Payload - ${emailName}</title>
                                    <style>
                                        body { font-family: monospace; padding: 20px; background: #1e1e1e; color: #d4d4d4; }
                                        pre { background: #2d2d30; padding: 15px; border-radius: 8px; overflow-x: auto; font-size: 12px; }
                                        .header { color: #4ec9b0; margin-bottom: 20px; }
                                        .copy-btn { background: #4ec9b0; color: black; padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer; margin-bottom: 10px; margin-right: 10px; }
                                        .info { background: #2d4a2d; padding: 10px; border-radius: 4px; margin-bottom: 10px; }
                                        .info-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; }
                                        .validated-badge { background: #4ec9b0; color: black; padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; }
                                    </style>
                                </head>
                                <body>
                                    <div class="header">
                                        <h2>Complete JSON Payload: ${emailName} <span class="validated-badge">✅ VALIDATED</span></h2>
                                        <div class="info">
                                            <div class="info-grid">
                                                <div><strong>Member:</strong> ${payload.member || 'N/A'}</div>
                                                <div><strong>Document Type:</strong> ${payload.documentType || 'N/A'}</div>
                                                <div><strong>File Name:</strong> ${payload.fileName || 'N/A'}</div>
                                                <div><strong>File Binary Size:</strong> ${payload.fileBinary ? (payload.fileBinary.length / 1024).toFixed(1) + ' KB' : 'N/A'}</div>
                                                <div><strong>Scheme Code:</strong> ${payload.schemeCode || 'N/A'}</div>
                                                <div><strong>Priority:</strong> ${payload.priority || 'N/A'}</div>
                                            </div>
                                        </div>
                                        <button class="copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('json-content').textContent)">📋 Copy JSON</button>
                                        <button class="copy-btn" onclick="downloadPayload()">💾 Download JSON</button>
                                    </div>
                                    <pre id="json-content">${JSON.stringify(payload, null, 2)}</pre>
                                    <script>
                                        function downloadPayload() {
                                            const content = document.getElementById('json-content').textContent;
                                            const blob = new Blob([content], { type: 'application/json' });
                                            const url = URL.createObjectURL(blob);
                                            const a = document.createElement('a');
                                            a.href = url;
                                            a.download = '${emailName}_payload.json';
                                            a.click();
                                            URL.revokeObjectURL(url);
                                        }
                                    <\/script>
                                </body>
                            </html>
                        `);
                    } catch (error) {
                        console.error('Error loading JSON payload:', error);
                        alert(`Failed to load JSON payload: ${error.message}`);
                    }
                }
                
                async function downloadCompletePayloads() {
                    try {
                        window.open('/download-complete-json-payloads', '_blank');
                    } catch (error) {
                        alert('Failed to download complete JSON payloads');
                    }
                }
                
                // Load initial data
                loadAllData();
            </script>
        </body>
    </html>
    """)

@app.route("/start-processing")
def start_processing():
    global processing_started, processing_complete, n8n_processing_started, n8n_processing_complete
    
    # Check if currently processing (not just if ever started)
    if processing_started and not processing_complete:
        return jsonify({"message": "⏳ Processing already in progress..."})
    
    # Reset all flags for new run
    processing_started = True
    processing_complete = False
    n8n_processing_started = True
    n8n_processing_complete = False
    
    threading.Thread(target=process_all_emails_with_n8n, daemon=True).start()
    return jsonify({"message": "✅ Processing started..."})


@app.route("/logs")
def stream_logs():
    def generate():
        yield f"data: [SYSTEM] Log stream connected\n\n"
        while True:
            try:
                msg = log_queue.get(timeout=1)
                yield f"data: {msg}\n\n"
            except queue.Empty:
                yield f"data: \n\n"
                time.sleep(1)
    return Response(generate(), mimetype="text/event-stream")


@app.route("/n8n-respond", methods=["POST"])
def n8n_respond():
    """
    Endpoint for n8n to call with processed results
    """
    data = request.get_json()
    folder = data.get("folder")
    result_text = data.get("result", "")

    if not folder:
        return jsonify({"status": "error", "message": "Missing folder"}), 400

    try:
        # Save result text in the folder
        file_path = f"{folder}/n8n_result.txt"
        upload_to_supabase(BUCKET_NAME, file_path, result_text, content_type="text/plain")
        n8n_status[folder]["status"] = "completed"
        log(f"[INFO] n8n response received for folder '{folder}' and saved to {file_path}")
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        log(f"[ERROR] Failed to save n8n response for folder '{folder}': {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/n8n-status")
def n8n_status_route():
    """
    Returns JSON with current n8n sending/completion status
    """
    return jsonify(n8n_status)


@app.route("/health")
def health():
    try:
        total_result = supabase_read.table('emails').select('id', count='exact').execute()
        total_count = total_result.count if hasattr(total_result, 'count') else 0

        success_result = supabase_read.table('emails').select('id', count='exact').eq('processing_status', 'success').execute()
        success_count = success_result.count if hasattr(success_result, 'count') else 0

        failed_result = supabase_read.table('emails').select('id', count='exact').eq('processing_status', 'failed').execute()
        failed_count = failed_result.count if hasattr(failed_result, 'count') else 0

        return jsonify({
            "status": "ok",
            "processing_started": processing_started,
            "processing_complete": processing_complete,
            "n8n_processing_started": n8n_processing_started,
            "n8n_processing_complete": n8n_processing_complete,
            "emails_total": total_count,
            "emails_success": success_count,
            "emails_failed": failed_count,
            "success_rate": round(success_count / total_count * 100, 2) if total_count > 0 else 0
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e),
            "processing_started": processing_started,
            "processing_complete": processing_complete
        }), 500


@app.route("/debug-supabase")
def debug_supabase():
    res = supabase_read.table('emails').select('*').limit(5).execute()
    return jsonify(res.data)

#new  routes
@app.route("/emails")
def get_emails():
    try:
        result = supabase_read.table('emails').select(
            'event_id, file_name, processing_status, classification_check, final_classification, confidence_score, json_payload, sender, payload_status'
        ).execute()
        
        for email in result.data:
            email['has_json_payload'] = bool(email.get('json_payload'))

            if not email.get('payload_status'):
                email['payload_status'] = 'pending'
        
        return jsonify(result.data)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/process-json-payloads")
def process_json_payloads():
    """Process all emails with JSON payloads to add file binary data"""
    try:
        result = BestmedPayloadProcessor.process_all_emails_with_json_payloads()
        
        return jsonify({
            "status": "success",
            "message": f"Processed {result['processed']} emails with {result['errors']} errors",
            "processed_count": result['processed'],
            "error_count": result['errors']
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route("/process-single-email/<email_name>")
def process_single_email(email_name):
    """Process a single email's JSON payload"""
    try:
        # URL decode the email name
        email_name = urllib.parse.unquote(email_name)
        
        #add .msg if it doesn't already have an extension
        if not email_name.endswith('.msg') and not email_name.endswith('.eml'):
            email_name += '.msg'
        
        log(f"[DEBUG] Processing single email: '{email_name}'")
        
        success = BestmedPayloadProcessor.process_email_json_payload(email_name)
        
        if success:
            return jsonify({
                "status": "success",
                "message": f"Successfully processed {email_name}",
                "email_name": email_name
            })
        else:
            return jsonify({
                "status": "error",
                "message": f"Failed to process {email_name}"
            }), 500
            
    except Exception as e:
        log(f"[ERROR] Error in process_single_email for '{email_name}': {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route("/json-payload-status")
def json_payload_status():
    try:
        result = supabase_read.table('emails').select(
            'event_id, file_name, json_payload, processing_status'
        ).not_.is_('json_payload', 'null').execute()
        
        status_summary = {
            "total_with_json": 0,
            "complete_with_binary": 0,
            "missing_binary": 0,
            "details": []
        }
        
        for email_record in result.data:
            status_summary["total_with_json"] += 1
            
            json_payload = email_record.get('json_payload')
            if isinstance(json_payload, str):
                json_payload = json.loads(json_payload)
            
            file_binary = json_payload.get('fileBinary', '')
            has_binary = file_binary and len(file_binary) > 100
            
            if has_binary:
                status_summary["complete_with_binary"] += 1
            else:
                status_summary["missing_binary"] += 1
            
            status_summary["details"].append({
                "event_id": email_record.get('event_id', 'N/A'),
                "file_name": email_record['file_name'],
                "has_file_binary": has_binary,
                "file_binary_size": len(file_binary) if file_binary else 0,
                "processing_status": email_record.get('processing_status', 'unknown'),
                "document_type": json_payload.get('documentType', 'unknown'),
                "member": json_payload.get('member', 'unknown')
            })
        
        return jsonify(status_summary)
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route("/json-payload/<path:email_name>")
def get_json_payload(email_name):
    """Get JSON payload for specific email - handles URL encoded names"""
    try:
        # URL decode the email name
        email_name = urllib.parse.unquote(email_name)
        
        # Add .msg if not present
        if not email_name.endswith('.msg') and not email_name.endswith('.eml'):
            email_name += '.msg'
        
        log(f"[DEBUG] Looking for email: '{email_name}'")
        
        result = supabase_read.table('emails').select(
            'json_payload'
        ).eq('file_name', email_name).execute()
        
        if result.data and result.data[0]['json_payload']:
            payload = result.data[0]['json_payload']
            if isinstance(payload, str):
                payload = json.loads(payload)

            formatted_payload = format_bestmed_payload(payload)

            # ✅ Use Response with custom JSON serialization to preserve order
            from flask import Response
            return Response(
                json.dumps(formatted_payload, indent=2),
                mimetype='application/json'
            )
        
        else:
            # Try case-insensitive search
            all_emails = supabase_read.table('emails').select('file_name, json_payload').execute()
            
            email_base_name = email_name.replace('.msg', '').replace('.eml', '').lower()

            for email_record in all_emails.data:
                # Remove extension from database record for comparison
                db_base_name = email_record['file_name'].replace('.msg', '').replace('.eml', '').lower()
                
                if db_base_name == email_base_name and email_record['json_payload']:
                    payload = email_record['json_payload']
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    log(f"[DEBUG] Found match: '{email_record['file_name']}' for requested '{email_name}'")
                    # ✅ Format the payload before returning
                    formatted_payload = format_bestmed_payload(payload)

                    # ✅ Use Response with custom JSON serialization to preserve order
                    from flask import Response
                    return Response(
                        json.dumps(formatted_payload, indent=2),
                        mimetype='application/json'
                    )
                
            
            
            log(f"[ERROR] No JSON payload found for: '{email_name}'")
            return jsonify({"error": f"JSON payload not found for email: {email_name}"}), 404
            
    except Exception as e:
        log(f"[ERROR] Failed to get JSON payload for '{email_name}': {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/download-complete-json-payloads")
def download_complete_json_payloads():
    """Download all complete JSON payloads (with file binary) as ZIP"""
    try:
        import zipfile
        from io import BytesIO
        
        # Get all emails with complete JSON payloads
        result = supabase_read.table('emails').select(
            'file_name, json_payload'
        ).not_.is_('json_payload', 'null').execute()
        
        if not result.data:
            return jsonify({"error": "No JSON payloads found"}), 404
        
        # Create ZIP file in memory
        memory_file = BytesIO()
        complete_payloads = 0
        
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            for email in result.data:
                json_payload = email['json_payload']
                if isinstance(json_payload, str):
                    json_payload = json.loads(json_payload)
                
                # Only include payloads with file binary
                file_binary = json_payload.get('fileBinary', '')
                if file_binary and len(file_binary) > 100:
                    # Create filename
                    email_name = email['file_name'].replace('.msg', '')
                    json_filename = f"{email_name}_complete_payload.json"

                    # ✅ Format the payload before adding to ZIP
                    formatted_payload = format_bestmed_payload(json_payload)
                    
                    # Add to ZIP
                    formatted_payload = format_bestmed_payload(json_payload)
                    zf.writestr(json_filename, json.dumps(json_payload, indent=2))
                    complete_payloads += 1
        
        if complete_payloads == 0:
            return jsonify({"error": "No complete JSON payloads found"}), 404
        
        memory_file.seek(0)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        return send_file(
            memory_file,
            as_attachment=True,
            download_name=f'bestmed_complete_payloads_{timestamp}.zip',
            mimetype='application/zip'
        )
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/events")
def get_events():
    """Get all events with Event IDs and payload status"""
    try:
        """
        result = supabase_server.table('emails').select(
            'event_id, file_name, sender, subject, payload_status, created_at'
        ).order('created_at', desc=True).execute()
        """
        result= supabase_read.table('emails').select(
            'event_id, file_name, sender, subject, payload_status, created_at'
            ).not_.is_('event_id', 'null').order('created_at', desc=True).execute()
        
        for record in result.data:
            if not record.get('payload_status'):
                record['payload_status'] = 'pending'
            if not record.get('created_at'):
                record['created_at']= datetime.now().isoformat()

        return jsonify(result.data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@app.route("/validate-single/<path:email_name>")
def validate_single(email_name):
    """Validate a single email"""
    try:
        email_name = urllib.parse.unquote(email_name)
        if not email_name.endswith('.msg') and not email_name.endswith('.eml'):
            email_name += '.msg'
        
        result = PayloadValidator.validate_email(email_name)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/validate-all")
def validate_all():
    """Validate all emails with JSON payloads"""
    try:
        result = PayloadValidator.validate_all()
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    host = os.getenv("FLASK_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_PORT", "5000"))
    log("[INFO] Server started. Visit the dashboard and click 'Start Processing' to begin.")
    app.run(host=host, port=port, debug=False)
