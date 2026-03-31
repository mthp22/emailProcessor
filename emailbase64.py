class EmailAttachmentProcessor:    
    # convert email .msg to base64
    @staticmethod
    def upload_msg_to_base64(file_path):
        try:
            if not os.path.exists(file_path):
                log(f"[ERROR] File not found: {file_path}")
                return None
            
            # Validate it's a .msg file
            if not file_path.lower().endswith('.msg'):
                log(f"[ERROR] File must be a .msg file: {file_path}")
                return None
            
            log(f"[FILE] Reading .msg file: {file_path}")

            # Read the file
            with open(file_path, 'rb') as f:
                file_data = f.read()
            
            if not file_data:
                log(f"[ERROR] File is empty: {file_path}")
                return None
            
            # Compress with gzip
            compressed_data = gzip.compress(file_data)
            
            # Convert to base64
            base64_data = base64.b64encode(compressed_data).decode('utf-8')
            
            log(f"[FILE] Successfully converted .msg file. Original: {len(file_data)} bytes, Compressed: {len(compressed_data)} bytes, Base64: {len(base64_data)} chars")
            
            return base64_data
        except Exception as e:
            log(f"[ERROR] Failed to convert .msg to base64: {e}")
            return None    

@staticmethod
def prompt_and_encode_msg():
    """Interactive function to prompt user for .msg file path and return base64"""
    try:
        print("\n=== MSG File to Base64 Converter ===")
        file_path = input("Enter the full path to your .msg file: ").strip()
        
        # Remove quotes if user copied path with quotes
        file_path = file_path.strip('"').strip("'")
        
        base64_data = EmailAttachmentProcessor.upload_msg_to_base64(file_path)
        
        if base64_data:
            print(f"\n✓ Successfully converted file!")
            print(f"Base64 length: {len(base64_data)} characters")
            
            # Option to save to file
            save_option = input("\nSave Base64 to file? (y/n): ").strip().lower()
            if save_option == 'y':
                output_file = input("Enter output filename (default: msg_base64.txt): ").strip()
                if not output_file:
                    output_file = "msg_base64.txt"
                
                with open(output_file, 'w') as f:
                    f.write(base64_data)
                print(f"✓ Saved to {output_file}")
            
            return base64_data
        else:
            print("\n✗ Failed to convert file")
            return None
            
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        return None
    except Exception as e:
        log(f"[ERROR] Interactive encoding failed: {e}")
        return None