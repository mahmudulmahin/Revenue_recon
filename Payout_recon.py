import streamlit as st
import pandas as pd
import io
import re
from datetime import datetime
import numpy as np

class ReconciliationProcessor:
    """Handles reconciliation logic for Forex and Futures data"""
    
    def parse_pasted_data(self, pasted_text):
        """Parse pasted data: Handle both Riseworks (Login Amount Email) and ALT (Login Amount) formats"""
        try:
            lines = pasted_text.strip().split('\n')
            riseworks_data = {}  # {email: {total_amount: X, logins: []}}
            crypto_data = []  # [{'Login': X, 'Amount': Y}]
            
            for line in lines:
                if not line.strip():
                    continue
                
                # Split by whitespace to get parts
                parts = line.strip().split()
                
                if len(parts) >= 2:
                    try:
                        # Check if this is Riseworks (3+ parts with email) or ALT (2 parts)
                        has_email = any('@' in part for part in parts)
                        
                        if has_email and len(parts) >= 3:
                            # Riseworks format: Login(s) Amount Email
                            amount_idx = -1
                            email_idx = -1
                            
                            for i, part in enumerate(parts):
                                if '$' in part or (part.replace(',', '').replace('.', '').isdigit() and '.' in part):
                                    amount_idx = i
                                elif '@' in part:
                                    email_idx = i
                                    break
                            
                            if amount_idx >= 0 and email_idx >= 0:
                                # Login(s) are everything before amount
                                login_parts = parts[:amount_idx]
                                login_field = ' '.join(login_parts)
                                
                                # Amount
                                amount_str = parts[amount_idx].replace('$', '').replace(',', '')
                                amount = float(amount_str)
                                
                                # Email
                                email = parts[email_idx]
                                
                                # Handle comma-separated logins
                                if ',' in login_field:
                                    login_ids = [login.strip() for login in login_field.split(',')]
                                else:
                                    login_ids = [login_field.strip()]
                                
                                # Group by email for Riseworks
                                if email not in riseworks_data:
                                    riseworks_data[email] = {'total_amount': 0, 'logins': []}
                                
                                riseworks_data[email]['total_amount'] += amount
                                riseworks_data[email]['logins'].extend(login_ids)
                        
                        elif len(parts) == 2:
                            # ALT format: Login Amount
                            login = parts[0].strip()
                            amount_str = parts[1].replace('$', '').replace(',', '')
                            amount = float(amount_str)
                            
                            crypto_data.append({
                                'Login': login,
                                'Amount': amount,
                                'Customer Email': None,  # No email for ALT
                                'Payment_Method': 'ALT'
                            })
                            
                    except (ValueError, IndexError) as e:
                        continue  # Skip malformed lines
            
            # Create final DataFrame
            final_data = []
            
            # Add Riseworks data (email-based) - Multiple rows per login with divided amounts
            for email, data in riseworks_data.items():
                total_amount = data['total_amount']
                unique_logins = list(set(data['logins']))  # Remove duplicates
                valid_logins = [login.strip() for login in unique_logins if login.strip()]
                
                if valid_logins:
                    # Divide total amount equally among logins for this email
                    amount_per_login = total_amount / len(valid_logins)
                    
                    for login in valid_logins:
                        final_data.append({
                            'Login': login,
                            'Amount': amount_per_login,  # Divided amount per login
                            'Customer Email': email,
                            'Payment_Method': 'Riseworks'
                        })
            
            # Add ALT data (login-based)
            final_data.extend(crypto_data)
            
            if not final_data:
                return None
            
            return pd.DataFrame(final_data)
            
        except Exception as e:
            raise Exception(f"Error parsing pasted data: {str(e)}")
    
    def reconcile_data(self, uploaded_df, pasted_df, section_type):
        """Perform reconciliation: Email-based for Riseworks, Login-based for ALT"""
        try:
            # Convert Login to string for both datasets
            uploaded_df = uploaded_df.copy()
            pasted_df = pasted_df.copy()
            
            uploaded_df['Login'] = uploaded_df['Login'].astype(str)
            pasted_df['Login'] = pasted_df['Login'].astype(str)
            
            # Separate Riseworks and ALT data from pasted data
            riseworks_pasted = pasted_df[pasted_df['Payment_Method'] == 'Riseworks'].copy()
            alt_pasted = pasted_df[pasted_df['Payment_Method'] == 'ALT'].copy()
            
            # Filter uploaded data by section type (Forex/Futures) AND payment method
            if section_type.lower() == 'forex':
                # Forex: Plan does NOT contain 'futures'
                section_mask = ~uploaded_df['Plan'].str.lower().str.contains('futures', na=False)
            else:  # Futures
                # Futures: Plan contains 'futures'
                section_mask = uploaded_df['Plan'].str.lower().str.contains('futures', na=False)
            
            # Apply section filter first
            uploaded_df_section = uploaded_df[section_mask].copy()
            
            # Now filter by payment methods that exist in pasted data
            pasted_payment_methods = set(pasted_df['Payment_Method'].unique())
            filtered_records = []
            
            # Filter by Riseworks if present in pasted data
            if 'Riseworks' in pasted_payment_methods:
                riseworks_records = uploaded_df_section[
                    uploaded_df_section['Payment Method'].str.lower().str.contains('riseworks', na=False)
                ]
                if len(riseworks_records) > 0:
                    filtered_records.append(riseworks_records)
            
            # Filter by ALT if present in pasted data  
            if 'ALT' in pasted_payment_methods:
                alt_records = uploaded_df_section[
                    uploaded_df_section['Payment Method'].str.upper().str.contains('USDC|USDT', na=False)
                ]
                if len(alt_records) > 0:
                    filtered_records.append(alt_records)
            
            # Use filtered data for reconciliation
            if filtered_records:
                uploaded_df = pd.concat(filtered_records, ignore_index=True)
            else:
                # If no matching records, create empty dataframe to show everything as paste-only
                uploaded_df = uploaded_df_section.iloc[0:0].copy()
            
            # Initialize results
            results = {
                'matched': pd.DataFrame(),
                'upload_only': pd.DataFrame(),
                'paste_only': pd.DataFrame(),
                'amount_differences': pd.DataFrame(),
                'summary': {}
            }
            
            all_matched = []
            all_amount_diff = []
            all_upload_only = []
            all_paste_only = []
            
            # RISEWORKS RECONCILIATION (Email-based)
            if len(riseworks_pasted) > 0:
                # Group by email for both datasets
                uploaded_email_groups = uploaded_df.groupby('Customer Email').agg({
                    'Disbursement Amount': 'sum',  # Sum all disbursement amounts per email
                    'Login': lambda x: list(x),
                    'Proof': 'first',
                    'Status': 'first',
                    'Requested Time': 'first',
                    'Approved Time': 'first',
                    'Disbursed Time': 'first'
                }).reset_index()
                
                riseworks_email_groups = riseworks_pasted.groupby('Customer Email').agg({
                    'Amount': 'sum',  # Sum the divided amounts to get original total
                    'Login': lambda x: list(x)
                }).reset_index()
                
                # Find email matches
                uploaded_emails = set(uploaded_email_groups['Customer Email'].str.lower().str.strip())
                riseworks_emails = set(riseworks_email_groups['Customer Email'].str.lower().str.strip())
                
                matched_emails = uploaded_emails.intersection(riseworks_emails)
                
                # Process Riseworks email matches
                for email in matched_emails:
                    upload_data = uploaded_email_groups[
                        uploaded_email_groups['Customer Email'].str.lower().str.strip() == email.lower()
                    ].iloc[0]
                    
                    paste_data = riseworks_email_groups[
                        riseworks_email_groups['Customer Email'].str.lower().str.strip() == email.lower()
                    ].iloc[0]
                    
                    excel_amount = upload_data['Disbursement Amount']
                    pasted_amount = paste_data['Amount']
                    amount_diff = abs(excel_amount - pasted_amount)
                    
                    record = {
                        'Payment_Method': 'Riseworks',
                        'Customer Email': upload_data['Customer Email'],
                        'Login': ', '.join(map(str, upload_data['Login'])),
                        'Excel_Disbursement_Amount': excel_amount,
                        'Pasted_Amount': pasted_amount,
                        'Proof': upload_data['Proof'],
                        'Status': upload_data['Status'],
                        'Requested Time': upload_data['Requested Time'],
                        'Approved Time': upload_data['Approved Time'],
                        'Disbursed Time': upload_data['Disbursed Time']
                    }
                    
                    if amount_diff <= 0.01:
                        all_matched.append(record)
                    else:
                        record['Amount_Difference'] = excel_amount - pasted_amount
                        all_amount_diff.append(record)
            
            # ALT RECONCILIATION (Login-based with summation)
            if len(alt_pasted) > 0:
                # Group by login and sum amounts for uploaded data
                uploaded_alt_groups = uploaded_df.groupby('Login').agg({
                    'Disbursement Amount': 'sum',
                    'Customer Email': 'first',
                    'Proof': 'first',
                    'Status': 'first',
                    'Requested Time': 'first',
                    'Approved Time': 'first',
                    'Disbursed Time': 'first'
                }).reset_index()
                
                # Group by login and sum amounts for pasted data
                alt_login_groups = alt_pasted.groupby('Login').agg({
                    'Amount': 'sum'
                }).reset_index()
                
                # Find login matches
                uploaded_logins = set(uploaded_alt_groups['Login'].astype(str))
                alt_logins = set(alt_login_groups['Login'].astype(str))
                
                matched_logins = uploaded_logins.intersection(alt_logins)
                
                # Process ALT login matches
                for login_key in matched_logins:
                    upload_data = uploaded_alt_groups[
                        uploaded_alt_groups['Login'].astype(str) == str(login_key)
                    ].iloc[0]
                    
                    paste_data = alt_login_groups[
                        alt_login_groups['Login'].astype(str) == str(login_key)
                    ].iloc[0]
                    
                    excel_amount = upload_data['Disbursement Amount']
                    pasted_amount = paste_data['Amount']
                    amount_diff = abs(excel_amount - pasted_amount)
                    
                    record = {
                        'Payment_Method': 'ALT',
                        'Login': login_key,
                        'Customer Email': upload_data['Customer Email'],
                        'Excel_Disbursement_Amount': excel_amount,
                        'Pasted_Amount': pasted_amount,
                        'Proof': upload_data['Proof'],
                        'Status': upload_data['Status'],
                        'Requested Time': upload_data['Requested Time'],
                        'Approved Time': upload_data['Approved Time'],
                        'Disbursed Time': upload_data['Disbursed Time']
                    }
                    
                    if amount_diff <= 0.01:
                        all_matched.append(record)
                    else:
                        record['Amount_Difference'] = excel_amount - pasted_amount
                        all_amount_diff.append(record)
            
            # Find unmatched records (upload only and paste only)
            all_matched_upload_emails = set()
            all_matched_upload_logins = set()
            
            for record in all_matched + all_amount_diff:
                if record['Payment_Method'] == 'Riseworks':
                    all_matched_upload_emails.add(record['Customer Email'].lower().strip())
                else:  # ALT
                    all_matched_upload_logins.add(record['Login'])
            
            # Upload only records
            for _, row in uploaded_df.iterrows():
                email_matched = row['Customer Email'].lower().strip() in all_matched_upload_emails
                login_matched = str(row['Login']) in all_matched_upload_logins
                
                if not (email_matched or login_matched):
                    all_upload_only.append({
                        'Login': row['Login'],
                        'Customer Email': row['Customer Email'],
                        'Amount': row['Amount'],
                        'Disbursement Amount': row['Disbursement Amount'],
                        'Proof': row['Proof'],
                        'Status': row['Status'],
                        'Requested Time': row['Requested Time'],
                        'Approved Time': row['Approved Time'],
                        'Disbursed Time': row['Disbursed Time']
                    })
            
            # Paste only records
            all_matched_paste_emails = set()
            all_matched_paste_logins = set()
            
            for record in all_matched + all_amount_diff:
                if record['Payment_Method'] == 'Riseworks' and 'Customer Email' in record:
                    all_matched_paste_emails.add(record['Customer Email'].lower().strip())
                elif record['Payment_Method'] == 'ALT':
                    all_matched_paste_logins.add(record['Login'])
            
            for _, row in pasted_df.iterrows():
                if row['Payment_Method'] == 'Riseworks':
                    if row['Customer Email'].lower().strip() not in all_matched_paste_emails:
                        all_paste_only.append({
                            'Payment_Method': 'Riseworks',
                            'Login': row['Login'],
                            'Customer Email': row['Customer Email'],
                            'Amount': row['Amount']
                        })
                else:  # ALT
                    if str(row['Login']) not in all_matched_paste_logins:
                        all_paste_only.append({
                            'Payment_Method': 'ALT',
                            'Login': row['Login'],
                            'Customer Email': None,
                            'Amount': row['Amount']
                        })
            
            # Create result DataFrames
            if all_matched:
                results['matched'] = pd.DataFrame(all_matched)
            if all_amount_diff:
                results['amount_differences'] = pd.DataFrame(all_amount_diff)
            if all_upload_only:
                results['upload_only'] = pd.DataFrame(all_upload_only)
            if all_paste_only:
                results['paste_only'] = pd.DataFrame(all_paste_only)
            
            # Generate summary
            results['summary'] = {
                'matched': len(all_matched),
                'upload_only': len(all_upload_only),
                'paste_only': len(all_paste_only),
                'amount_differences': len(all_amount_diff),
                'total_upload_records': len(uploaded_df),
                'total_pasted_records': len(pasted_df),
                'section_type': section_type
            }
            
            return results
            
        except Exception as e:
            raise Exception(f"Error during reconciliation: {str(e)}")

# Initialize session state
if 'uploaded_data' not in st.session_state:
    st.session_state.uploaded_data = None
if 'forex_data' not in st.session_state:
    st.session_state.forex_data = None
if 'futures_data' not in st.session_state:
    st.session_state.futures_data = None
if 'forex_results' not in st.session_state:
    st.session_state.forex_results = None
if 'futures_results' not in st.session_state:
    st.session_state.futures_results = None
if 'forex_processed' not in st.session_state:
    st.session_state.forex_processed = False
if 'futures_processed' not in st.session_state:
    st.session_state.futures_processed = False

st.title("Sequential Reconciliation Tool")
st.markdown("Upload your data file and process Forex and Futures reconciliation separately")

# File Upload Section
st.header("📁 Step 1: Upload Data File")
uploaded_file = st.file_uploader(
    "Choose Excel or CSV file", 
    type=['xlsx', 'xls', 'csv'],
    help="File must contain columns: Login, Customer Email, Plan, Amount, Disbursement Amount, Proof, Status, and Time columns"
)

if uploaded_file is not None:
    try:
        # Read the uploaded file
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        
        # Validate required columns
        required_columns = ['Login', 'Customer Email', 'Plan', 'Amount', 'Disbursement Amount', 'Proof', 'Status', 'Requested Time', 'Approved Time', 'Disbursed Time']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            st.error(f"Missing required columns: {', '.join(missing_columns)}")
            st.stop()
        
        # Store uploaded data
        st.session_state.uploaded_data = df
        
        # Display file info
        st.success(f"✅ File uploaded successfully! ({len(df)} records)")
        
        # Filter data by Plan (futures check - case insensitive)
        futures_mask = df['Plan'].str.lower().str.contains('futures', na=False)
        forex_uploaded = df[~futures_mask].copy()
        futures_uploaded = df[futures_mask].copy()
        
        # Filter for disbursed entries only for main metrics
        disbursed_df = df[df['Status'].str.lower().str.contains('disbursed', na=False)].copy()
        disbursed_forex = disbursed_df[~disbursed_df['Plan'].str.lower().str.contains('futures', na=False)].copy()
        disbursed_futures = disbursed_df[disbursed_df['Plan'].str.lower().str.contains('futures', na=False)].copy()
        
        # Convert date columns to datetime for processing
        df['Approved Time'] = pd.to_datetime(df['Approved Time'], errors='coerce')
        df['Disbursed Time'] = pd.to_datetime(df['Disbursed Time'], errors='coerce')
        
        # Calculate date ranges
        approved_dates = df['Approved Time'].dropna()
        disbursed_dates = df['Disbursed Time'].dropna()
        
        earliest_approved = approved_dates.min() if not approved_dates.empty else None
        latest_approved = approved_dates.max() if not approved_dates.empty else None
        earliest_disbursed = disbursed_dates.min() if not disbursed_dates.empty else None
        latest_disbursed = disbursed_dates.max() if not disbursed_dates.empty else None
        
        # Payment method classification (disbursed only)
        alt_mask = disbursed_df['Payment Method'].str.upper().str.contains('USDC|USDT', na=False)
        riseworks_mask = disbursed_df['Payment Method'].str.lower().str.contains('riseworks', na=False)
        
        alt_data = disbursed_df[alt_mask]
        riseworks_data = disbursed_df[riseworks_mask]
        
        # Show Plan distribution with disbursement amounts (disbursed only)
        st.subheader("📊 Disbursed Data Overview")
        
        # Forex section breakdown
        st.write("**💱 Forex Section**")
        forex_alt = disbursed_forex[disbursed_forex['Payment Method'].str.upper().str.contains('USDC|USDT', na=False)]
        forex_riseworks = disbursed_forex[disbursed_forex['Payment Method'].str.lower().str.contains('riseworks', na=False)]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            forex_disbursement = disbursed_forex['Disbursement Amount'].sum() if len(disbursed_forex) > 0 else 0
            st.metric(
                "Total Forex Records", 
                len(disbursed_forex),
                delta=f"${forex_disbursement:,.2f}" if forex_disbursement > 0 else "$0.00"
            )
        with col2:
            forex_alt_disbursement = forex_alt['Disbursement Amount'].sum() if len(forex_alt) > 0 else 0
            st.metric(
                "┗━ ALT", 
                len(forex_alt),
                delta=f"${forex_alt_disbursement:,.2f}" if forex_alt_disbursement > 0 else "$0.00"
            )
        with col3:
            forex_riseworks_disbursement = forex_riseworks['Disbursement Amount'].sum() if len(forex_riseworks) > 0 else 0
            st.metric(
                "┗━ Riseworks", 
                len(forex_riseworks),
                delta=f"${forex_riseworks_disbursement:,.2f}" if forex_riseworks_disbursement > 0 else "$0.00"
            )
        
        st.write("")  # Add spacing
        
        # Futures section breakdown  
        st.write("**📈 Futures Section**")
        futures_alt = disbursed_futures[disbursed_futures['Payment Method'].str.upper().str.contains('USDC|USDT', na=False)]
        futures_riseworks = disbursed_futures[disbursed_futures['Payment Method'].str.lower().str.contains('riseworks', na=False)]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            futures_disbursement = disbursed_futures['Disbursement Amount'].sum() if len(disbursed_futures) > 0 else 0
            st.metric(
                "Total Futures Records", 
                len(disbursed_futures),
                delta=f"${futures_disbursement:,.2f}" if futures_disbursement > 0 else "$0.00"
            )
        with col2:
            futures_alt_disbursement = futures_alt['Disbursement Amount'].sum() if len(futures_alt) > 0 else 0
            st.metric(
                "┗━ ALT", 
                len(futures_alt),
                delta=f"${futures_alt_disbursement:,.2f}" if futures_alt_disbursement > 0 else "$0.00"
            )
        with col3:
            futures_riseworks_disbursement = futures_riseworks['Disbursement Amount'].sum() if len(futures_riseworks) > 0 else 0
            st.metric(
                "┗━ Riseworks", 
                len(futures_riseworks),
                delta=f"${futures_riseworks_disbursement:,.2f}" if futures_riseworks_disbursement > 0 else "$0.00"
            )
        
        # Status breakdown section
        st.subheader("📈 Status Breakdown (All Records)")
        status_summary = df.groupby('Status').agg({
            'Status': 'count',
            'Disbursement Amount': 'sum',
            'Amount': 'sum'
        }).rename(columns={'Status': 'Count'})
        
        # Create dynamic columns based on unique statuses
        unique_statuses = df['Status'].unique()
        if len(unique_statuses) > 0:
            # Create columns dynamically
            status_cols = st.columns(min(len(unique_statuses), 4))  # Max 4 columns per row
            
            for i, status in enumerate(unique_statuses):
                if pd.isna(status):
                    continue
                    
                col_idx = i % len(status_cols)
                with status_cols[col_idx]:
                    status_data = df[df['Status'] == status]
                    count = len(status_data)
                    disbursement_sum = status_data['Disbursement Amount'].sum()
                    amount_sum = status_data['Amount'].sum()
                    
                    st.metric(
                        f"{status}",
                        f"{count} records"
                    )
                    st.info(f"**Total Amount**: ${amount_sum:,.2f}")
                    st.info(f"**Disbursed Amount**: ${disbursement_sum:,.2f}")
            
            # Show detailed status table
            with st.expander("📋 Detailed Status Breakdown"):
                status_detail = df.groupby(['Status', 'Plan']).agg({
                    'Status': 'count',
                    'Disbursement Amount': 'sum',
                    'Amount': 'sum'
                }).rename(columns={'Status': 'Count'}).reset_index()
                st.dataframe(status_detail, use_container_width=True)
        
        # Show date ranges
        st.subheader("📅 Date Ranges")
        col1, col2 = st.columns(2)
        
        with col1:
            if earliest_approved and latest_approved:
                st.info(f"**Approved Time Range:**\n{earliest_approved.strftime('%Y-%m-%d')} to {latest_approved.strftime('%Y-%m-%d')}")
            else:
                st.info("**Approved Time Range:** No data")
        
        with col2:
            if earliest_disbursed and latest_disbursed:
                st.info(f"**Disbursed Time Range:**\n{earliest_disbursed.strftime('%Y-%m-%d')} to {latest_disbursed.strftime('%Y-%m-%d')}")
            else:
                st.info("**Disbursed Time Range:** No data")
        
        # Display sample of uploaded data
        with st.expander("View Sample Data"):
            st.subheader("Forex Sample (First 5 records)")
            if len(forex_uploaded) > 0:
                st.dataframe(forex_uploaded.head())
            else:
                st.info("No Forex records found")
            
            st.subheader("Futures Sample (First 5 records)")
            if len(futures_uploaded) > 0:
                st.dataframe(futures_uploaded.head())
            else:
                st.info("No Futures records found")
            
            st.subheader("Payment Method Breakdown (All Records)")
            payment_breakdown = df['Payment Method'].value_counts().head(10)
            if len(payment_breakdown) > 0:
                st.bar_chart(payment_breakdown)
            else:
                st.info("No payment method data found")
            
            st.subheader("Status Distribution")
            status_breakdown = df['Status'].value_counts()
            if len(status_breakdown) > 0:
                st.bar_chart(status_breakdown)
            else:
                st.info("No status data found")
        
    except Exception as e:
        st.error(f"Error reading file: {str(e)}")
        st.stop()

# Only show reconciliation sections if file is uploaded
if st.session_state.uploaded_data is not None:
    
    # Forex Section
    st.header("💱 Step 2: Forex Reconciliation")
    
    # Forex status indicator
    if st.session_state.forex_processed:
        st.success("✅ Forex reconciliation completed")
    else:
        st.info("📋 Paste your Forex data below and click Proceed")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏦 Riseworks Data")
        forex_riseworks_area = st.text_area(
            "Paste Riseworks Data (Login, Amount, Email format)",
            height=120,
            placeholder="Example:\n13684620\t$3,172.96\tmattiaantonioni56@gmail.com\n13684617\t$3,095.78\tRedarikala07@gmail.com",
            key="forex_riseworks_paste"
        )
    
    with col2:
        st.subheader("₿ ALT Data")
        forex_alt_area = st.text_area(
            "Paste ALT Data (Login, Amount format)",
            height=120,
            placeholder="Example:\n11432426\t28.15\n13657842\t147.69\n11488619\t388.87",
            key="forex_alt_paste"
        )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        forex_proceed = st.button("🔄 Proceed Forex", type="primary")
    
    if forex_proceed and (forex_riseworks_area.strip() or forex_alt_area.strip()):
        try:
            processor = ReconciliationProcessor()
            
            # Parse both types of data and combine them
            all_forex_pasted = []
            
            if forex_riseworks_area.strip():
                riseworks_data = processor.parse_pasted_data(forex_riseworks_area)
                if riseworks_data is not None and len(riseworks_data) > 0:
                    all_forex_pasted.append(riseworks_data)
            
            if forex_alt_area.strip():
                alt_data = processor.parse_pasted_data(forex_alt_area)
                if alt_data is not None and len(alt_data) > 0:
                    all_forex_pasted.append(alt_data)
            
            if all_forex_pasted:
                # Combine all pasted data
                import pandas as pd
                forex_pasted_data = pd.concat(all_forex_pasted, ignore_index=True)
            else:
                forex_pasted_data = None
            
            if forex_pasted_data is not None and len(forex_pasted_data) > 0:
                # Get Forex records from uploaded file (exclude entries with 'futures' in plan)
                futures_mask = st.session_state.uploaded_data['Plan'].str.lower().str.contains('futures', na=False)
                forex_uploaded = st.session_state.uploaded_data[~futures_mask].copy()
                
                # Filter forex data for disbursed entries only for reconciliation
                forex_uploaded_disbursed = forex_uploaded[forex_uploaded['Status'].str.lower().str.contains('disbursed', na=False)].copy()
                
                # Perform reconciliation using only disbursed forex entries
                results = processor.reconcile_data(forex_uploaded_disbursed, forex_pasted_data, "Forex")
                st.session_state.forex_results = results
                st.session_state.forex_processed = True
                st.rerun()
            else:
                st.error("No valid Forex data found in pasted content")
                
        except Exception as e:
            st.error(f"Error processing Forex data: {str(e)}")
    elif forex_proceed:
        st.error("Please paste either Riseworks or ALT data (or both) to proceed")
    
    # Show Forex Results
    if st.session_state.forex_results is not None:
        # Show Pasted Data Summary first
        with st.expander("📋 Pasted Data Summary", expanded=True):
            # Get the pasted data from session or reconstruct it
            processor = ReconciliationProcessor()
            
            # Parse both types of data again to show summary
            all_forex_pasted = []
            
            riseworks_area = st.session_state.get('forex_riseworks_paste', '')
            alt_area = st.session_state.get('forex_alt_paste', '')
            
            if riseworks_area and riseworks_area.strip():
                riseworks_data = processor.parse_pasted_data(riseworks_area)
                if riseworks_data is not None and len(riseworks_data) > 0:
                    all_forex_pasted.append(riseworks_data)
            
            if alt_area and alt_area.strip():
                alt_data = processor.parse_pasted_data(alt_area)
                if alt_data is not None and len(alt_data) > 0:
                    all_forex_pasted.append(alt_data)
            
            if all_forex_pasted:
                import pandas as pd
                combined_pasted = pd.concat(all_forex_pasted, ignore_index=True)
                
                # Separate by payment method
                riseworks_pasted = combined_pasted[combined_pasted['Payment_Method'] == 'Riseworks']
                alt_pasted = combined_pasted[combined_pasted['Payment_Method'] == 'ALT']
                
                # Calculate summaries
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    riseworks_count = len(riseworks_pasted)
                    riseworks_amount = riseworks_pasted['Amount'].sum() if riseworks_count > 0 else 0
                    st.metric(
                        "🏦 Riseworks",
                        f"{riseworks_count} records",
                        delta=f"${riseworks_amount:,.2f}" if riseworks_amount > 0 else "$0.00"
                    )
                
                with col2:
                    alt_count = len(alt_pasted)
                    alt_amount = alt_pasted['Amount'].sum() if alt_count > 0 else 0
                    st.metric(
                        "₿ ALT",
                        f"{alt_count} records", 
                        delta=f"${alt_amount:,.2f}" if alt_amount > 0 else "$0.00"
                    )
                
                with col3:
                    total_count = len(combined_pasted)
                    total_amount = combined_pasted['Amount'].sum()
                    st.metric(
                        "📊 Total GSheet",
                        f"{total_count} records",
                        delta=f"${total_amount:,.2f}" if total_amount > 0 else "$0.00"
                    )
                
                # Breakdown by unique logins/emails
                st.subheader("📈 Breakdown Details")
                col1, col2 = st.columns(2)
                
                with col1:
                    if riseworks_count > 0:
                        unique_emails = riseworks_pasted['Customer Email'].nunique()
                        st.info(f"**Riseworks**: {unique_emails} unique emails, {riseworks_count} total entries")
                
                with col2:
                    if alt_count > 0:
                        unique_logins = alt_pasted['Login'].nunique() 
                        st.info(f"**ALT**: {unique_logins} unique logins, {alt_count} total entries")
        
        with st.expander("📊 Forex Reconciliation Summary", expanded=True):
            results = st.session_state.forex_results
            
            # Get API data (uploaded file) - Forex disbursed only
            forex_mask = st.session_state.uploaded_data['Plan'].str.lower().str.contains('forex', na=False) | ~st.session_state.uploaded_data['Plan'].str.lower().str.contains('futures', na=False)
            forex_api_data = st.session_state.uploaded_data[forex_mask]
            forex_api_disbursed = forex_api_data[forex_api_data['Status'].str.lower().str.contains('disbursed', na=False)]
            
            # Calculate API data totals by payment method
            forex_api_alt = forex_api_disbursed[forex_api_disbursed['Payment Method'].str.upper().str.contains('USDC|USDT', na=False)]
            forex_api_riseworks = forex_api_disbursed[forex_api_disbursed['Payment Method'].str.lower().str.contains('riseworks', na=False)]
            
            # Reconstruct GSheet data
            processor = ReconciliationProcessor()
            all_forex_gsheet = []
            
            riseworks_area = st.session_state.get('forex_riseworks_paste', '')
            alt_area = st.session_state.get('forex_alt_paste', '')
            
            if riseworks_area and riseworks_area.strip():
                riseworks_data = processor.parse_pasted_data(riseworks_area)
                if riseworks_data is not None and len(riseworks_data) > 0:
                    all_forex_gsheet.append(riseworks_data)
            
            if alt_area and alt_area.strip():
                alt_data = processor.parse_pasted_data(alt_area)
                if alt_data is not None and len(alt_data) > 0:
                    all_forex_gsheet.append(alt_data)
            
            if all_forex_gsheet:
                import pandas as pd
                combined_gsheet = pd.concat(all_forex_gsheet, ignore_index=True)
                forex_gsheet_riseworks = combined_gsheet[combined_gsheet['Payment_Method'] == 'Riseworks']
                forex_gsheet_alt = combined_gsheet[combined_gsheet['Payment_Method'] == 'ALT']
            else:
                combined_gsheet = pd.DataFrame()
                forex_gsheet_riseworks = pd.DataFrame()
                forex_gsheet_alt = pd.DataFrame()
            
            # Main Summary
            st.subheader("🔄 Total Reconciliation Overview")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Matched", results['summary']['matched'])
            with col2:
                st.metric("API Only", results['summary']['upload_only'])
            with col3:
                st.metric("GSheet Only", results['summary']['paste_only'])
            with col4:
                st.metric("Amount Diff", results['summary']['amount_differences'])
            
            # Data Source Comparison
            st.subheader("📊 Data Source Breakdown")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**🔗 API Data (Disbursed Only)**")
                
                # API Totals
                api_alt_count = len(forex_api_alt)
                api_alt_amount = forex_api_alt['Disbursement Amount'].sum() if api_alt_count > 0 else 0
                api_riseworks_count = len(forex_api_riseworks) 
                api_riseworks_amount = forex_api_riseworks['Disbursement Amount'].sum() if api_riseworks_count > 0 else 0
                api_total_count = api_alt_count + api_riseworks_count
                api_total_amount = api_alt_amount + api_riseworks_amount
                
                st.metric("Total API Records", api_total_count, delta=f"${api_total_amount:,.2f}")
                st.info(f"┗━ **ALT**: {api_alt_count} records, ${api_alt_amount:,.2f}")
                st.info(f"┗━ **Riseworks**: {api_riseworks_count} records, ${api_riseworks_amount:,.2f}")
            
            with col2:
                st.write("**📋 GSheet Data**")
                
                # GSheet Totals
                gsheet_alt_count = len(forex_gsheet_alt)
                gsheet_alt_amount = forex_gsheet_alt['Amount'].sum() if gsheet_alt_count > 0 else 0
                gsheet_riseworks_count = len(forex_gsheet_riseworks)
                gsheet_riseworks_amount = forex_gsheet_riseworks['Amount'].sum() if gsheet_riseworks_count > 0 else 0
                gsheet_total_count = gsheet_alt_count + gsheet_riseworks_count
                gsheet_total_amount = gsheet_alt_amount + gsheet_riseworks_amount
                
                st.metric("Total GSheet Records", gsheet_total_count, delta=f"${gsheet_total_amount:,.2f}")
                st.info(f"┗━ **ALT**: {gsheet_alt_count} records, ${gsheet_alt_amount:,.2f}")
                st.info(f"┗━ **Riseworks**: {gsheet_riseworks_count} records, ${gsheet_riseworks_amount:,.2f}")
            
            # Reconciliation Status
            st.subheader("⚖️ Reconciliation Status")
            
            if len(results['upload_only']) > 0:
                st.subheader("⚠️ API Only (Not in GSheet Data)")
                st.dataframe(results['upload_only'])
            
            if len(results['paste_only']) > 0:
                st.subheader("⚠️ GSheet Only (Not in API Data)")
                st.dataframe(results['paste_only'])
            
            if len(results['amount_differences']) > 0:
                st.subheader("💰 Amount Discrepancies")
                st.dataframe(results['amount_differences'])
    
    st.divider()
    
    # Futures Section
    st.header("📈 Step 3: Futures Reconciliation")
    
    # Futures status indicator
    if st.session_state.futures_processed:
        st.success("✅ Futures reconciliation completed")
    else:
        st.info("📋 Paste your Futures data below and click Proceed")
    
    # Futures data input areas
    col1, col2 = st.columns(2, gap="medium")
    
    with col1:
        st.subheader("🏦 Riseworks Data")
        futures_riseworks_paste_area = st.text_area(
            "Paste Riseworks Data (Login, Amount, Email format)",
            height=180,
            placeholder="Example:\n13684620\t$3,172.96\tmattiaantonioni56@gmail.com\n13684617\t$3,095.78\tRedarikala07@gmail.com",
            key="futures_riseworks_paste"
        )
    
    with col2:
        st.subheader("₿ ALT Data") 
        futures_alt_paste_area = st.text_area(
            "Paste ALT Data (Login, Amount format)",
            height=180,
            placeholder="Example:\n13684620\t$3,172.96\n13684617\t$3,095.78",
            key="futures_alt_paste"
        )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        futures_proceed = st.button("🔄 Proceed Futures", type="primary")
    
    if futures_proceed and (futures_riseworks_paste_area.strip() or futures_alt_paste_area.strip()):
        try:
            processor = ReconciliationProcessor()
            
            # Parse both types of futures data and combine
            all_futures_pasted = []
            
            if futures_riseworks_paste_area.strip():
                riseworks_data = processor.parse_pasted_data(futures_riseworks_paste_area)
                if riseworks_data is not None and len(riseworks_data) > 0:
                    all_futures_pasted.append(riseworks_data)
            
            if futures_alt_paste_area.strip():
                alt_data = processor.parse_pasted_data(futures_alt_paste_area)
                if alt_data is not None and len(alt_data) > 0:
                    all_futures_pasted.append(alt_data)
            
            if all_futures_pasted:
                import pandas as pd
                futures_pasted_data = pd.concat(all_futures_pasted, ignore_index=True)
                
                # Store in session state for summary display (using different keys to avoid widget conflicts)
                st.session_state['futures_riseworks_data'] = futures_riseworks_paste_area
                st.session_state['futures_alt_data'] = futures_alt_paste_area
                
                # Get Futures records from uploaded file (entries containing 'futures' in plan)
                futures_mask = st.session_state.uploaded_data['Plan'].str.lower().str.contains('futures', na=False)
                futures_uploaded = st.session_state.uploaded_data[futures_mask].copy()
                
                # Filter futures data for disbursed entries only for reconciliation
                futures_uploaded_disbursed = futures_uploaded[futures_uploaded['Status'].str.lower().str.contains('disbursed', na=False)].copy()
                
                # Perform reconciliation using only disbursed futures entries
                results = processor.reconcile_data(futures_uploaded_disbursed, futures_pasted_data, "Futures")
                st.session_state.futures_results = results
                st.session_state.futures_processed = True
                st.rerun()
            else:
                st.error("No valid Futures data found in pasted content")
                
        except Exception as e:
            st.error(f"Error processing Futures data: {str(e)}")
    elif futures_proceed:
        st.error("Please paste either Riseworks or ALT data (or both) to proceed")
    
    # Show Futures Results
    if st.session_state.futures_results is not None:
        # Show Pasted Data Summary first
        with st.expander("📋 Futures Pasted Data Summary", expanded=True):
            # Get the pasted data from session or reconstruct it
            processor = ReconciliationProcessor()
            
            # Parse both types of data again to show summary
            all_futures_pasted = []
            
            riseworks_area = st.session_state.get('futures_riseworks_data', '')
            alt_area = st.session_state.get('futures_alt_data', '')
            
            if riseworks_area and riseworks_area.strip():
                riseworks_data = processor.parse_pasted_data(riseworks_area)
                if riseworks_data is not None and len(riseworks_data) > 0:
                    all_futures_pasted.append(riseworks_data)
            
            if alt_area and alt_area.strip():
                alt_data = processor.parse_pasted_data(alt_area)
                if alt_data is not None and len(alt_data) > 0:
                    all_futures_pasted.append(alt_data)
            
            if all_futures_pasted:
                import pandas as pd
                combined_pasted = pd.concat(all_futures_pasted, ignore_index=True)
                
                # Separate by payment method
                riseworks_pasted = combined_pasted[combined_pasted['Payment_Method'] == 'Riseworks']
                alt_pasted = combined_pasted[combined_pasted['Payment_Method'] == 'ALT']
                
                # Calculate summaries
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    riseworks_count = len(riseworks_pasted)
                    riseworks_amount = riseworks_pasted['Amount'].sum() if riseworks_count > 0 else 0
                    st.metric(
                        "🏦 Riseworks",
                        f"{riseworks_count} records",
                        delta=f"${riseworks_amount:,.2f}" if riseworks_amount > 0 else "$0.00"
                    )
                
                with col2:
                    alt_count = len(alt_pasted)
                    alt_amount = alt_pasted['Amount'].sum() if alt_count > 0 else 0
                    st.metric(
                        "₿ ALT",
                        f"{alt_count} records", 
                        delta=f"${alt_amount:,.2f}" if alt_amount > 0 else "$0.00"
                    )
                
                with col3:
                    total_count = len(combined_pasted)
                    total_amount = combined_pasted['Amount'].sum()
                    st.metric(
                        "📊 Total GSheet",
                        f"{total_count} records",
                        delta=f"${total_amount:,.2f}" if total_amount > 0 else "$0.00"
                    )
                
                # Breakdown by unique logins/emails
                st.subheader("📈 Breakdown Details")
                col1, col2 = st.columns(2)
                
                with col1:
                    if riseworks_count > 0:
                        unique_emails = riseworks_pasted['Customer Email'].nunique()
                        st.info(f"**Riseworks**: {unique_emails} unique emails, {riseworks_count} total entries")
                
                with col2:
                    if alt_count > 0:
                        unique_logins = alt_pasted['Login'].nunique() 
                        st.info(f"**ALT**: {unique_logins} unique logins, {alt_count} total entries")
        
        with st.expander("📊 Futures Reconciliation Summary", expanded=True):
            results = st.session_state.futures_results
            
            # Get API data (uploaded file) - Futures disbursed only
            futures_mask = st.session_state.uploaded_data['Plan'].str.lower().str.contains('futures', na=False)
            futures_api_data = st.session_state.uploaded_data[futures_mask]
            futures_api_disbursed = futures_api_data[futures_api_data['Status'].str.lower().str.contains('disbursed', na=False)]
            
            # Calculate API data totals by payment method
            futures_api_alt = futures_api_disbursed[futures_api_disbursed['Payment Method'].str.upper().str.contains('USDC|USDT', na=False)]
            futures_api_riseworks = futures_api_disbursed[futures_api_disbursed['Payment Method'].str.lower().str.contains('riseworks', na=False)]
            
            # Reconstruct GSheet data
            processor = ReconciliationProcessor()
            all_futures_gsheet = []
            
            riseworks_area = st.session_state.get('futures_riseworks_data', '')
            alt_area = st.session_state.get('futures_alt_data', '')
            
            if riseworks_area and riseworks_area.strip():
                riseworks_data = processor.parse_pasted_data(riseworks_area)
                if riseworks_data is not None and len(riseworks_data) > 0:
                    all_futures_gsheet.append(riseworks_data)
            
            if alt_area and alt_area.strip():
                alt_data = processor.parse_pasted_data(alt_area)
                if alt_data is not None and len(alt_data) > 0:
                    all_futures_gsheet.append(alt_data)
            
            if all_futures_gsheet:
                import pandas as pd
                combined_gsheet = pd.concat(all_futures_gsheet, ignore_index=True)
                futures_gsheet_riseworks = combined_gsheet[combined_gsheet['Payment_Method'] == 'Riseworks']
                futures_gsheet_alt = combined_gsheet[combined_gsheet['Payment_Method'] == 'ALT']
            else:
                combined_gsheet = pd.DataFrame()
                futures_gsheet_riseworks = pd.DataFrame()
                futures_gsheet_alt = pd.DataFrame()
            
            # Main Summary
            st.subheader("🔄 Total Reconciliation Overview")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Matched", results['summary']['matched'])
            with col2:
                st.metric("API Only", results['summary']['upload_only'])
            with col3:
                st.metric("GSheet Only", results['summary']['paste_only'])
            with col4:
                st.metric("Amount Diff", results['summary']['amount_differences'])
            
            # Data Source Comparison
            st.subheader("📊 Data Source Breakdown")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**🔗 API Data (Disbursed Only)**")
                
                # API Totals
                api_alt_count = len(futures_api_alt)
                api_alt_amount = futures_api_alt['Disbursement Amount'].sum() if api_alt_count > 0 else 0
                api_riseworks_count = len(futures_api_riseworks) 
                api_riseworks_amount = futures_api_riseworks['Disbursement Amount'].sum() if api_riseworks_count > 0 else 0
                api_total_count = api_alt_count + api_riseworks_count
                api_total_amount = api_alt_amount + api_riseworks_amount
                
                st.metric("Total API Records", api_total_count, delta=f"${api_total_amount:,.2f}")
                st.info(f"┗━ **ALT**: {api_alt_count} records, ${api_alt_amount:,.2f}")
                st.info(f"┗━ **Riseworks**: {api_riseworks_count} records, ${api_riseworks_amount:,.2f}")
            
            with col2:
                st.write("**📋 GSheet Data**")
                
                # GSheet Totals
                gsheet_alt_count = len(futures_gsheet_alt)
                gsheet_alt_amount = futures_gsheet_alt['Amount'].sum() if gsheet_alt_count > 0 else 0
                gsheet_riseworks_count = len(futures_gsheet_riseworks)
                gsheet_riseworks_amount = futures_gsheet_riseworks['Amount'].sum() if gsheet_riseworks_count > 0 else 0
                gsheet_total_count = gsheet_alt_count + gsheet_riseworks_count
                gsheet_total_amount = gsheet_alt_amount + gsheet_riseworks_amount
                
                st.metric("Total GSheet Records", gsheet_total_count, delta=f"${gsheet_total_amount:,.2f}")
                st.info(f"┗━ **ALT**: {gsheet_alt_count} records, ${gsheet_alt_amount:,.2f}")
                st.info(f"┗━ **Riseworks**: {gsheet_riseworks_count} records, ${gsheet_riseworks_amount:,.2f}")
            
            # Reconciliation Status
            st.subheader("⚖️ Reconciliation Status")
            
            if len(results['upload_only']) > 0:
                st.subheader("⚠️ API Only (Not in GSheet Data)")
                st.dataframe(results['upload_only'])
            
            if len(results['paste_only']) > 0:
                st.subheader("⚠️ GSheet Only (Not in API Data)")
                st.dataframe(results['paste_only'])
            
            if len(results['amount_differences']) > 0:
                st.subheader("💰 Amount Discrepancies")
                st.dataframe(results['amount_differences'])
    
    st.divider()
    
    # Master Summary Section
    st.header("📋 Step 4: Master Summary")
    
    # Only show summarize button if at least one section is processed
    if st.session_state.forex_processed or st.session_state.futures_processed:
        col1, col2 = st.columns([1, 4])
        with col1:
            summarize_btn = st.button("📊 Generate Master Summary", type="primary")
        
        if summarize_btn:
            st.subheader("🔍 Comprehensive Reconciliation Summary")
            
            # Combined metrics
            total_matched = 0
            total_upload_only = 0
            total_paste_only = 0
            total_amount_diff = 0
            
            if st.session_state.forex_results:
                forex_summary = st.session_state.forex_results['summary']
                total_matched += forex_summary['matched']
                total_upload_only += forex_summary['upload_only']
                total_paste_only += forex_summary['paste_only']
                total_amount_diff += forex_summary['amount_differences']
            
            if st.session_state.futures_results:
                futures_summary = st.session_state.futures_results['summary']
                total_matched += futures_summary['matched']
                total_upload_only += futures_summary['upload_only']
                total_paste_only += futures_summary['paste_only']
                total_amount_diff += futures_summary['amount_differences']
            
            
            # Display combined metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Matched", total_matched)
            with col2:
                st.metric("Total Upload Only", total_upload_only)
            with col3:
                st.metric("Total Paste Only", total_paste_only)
            with col4:
                st.metric("Total Amount Diff", total_amount_diff)
            
            # Section-wise breakdown
            st.subheader("📊 Section-wise Breakdown")
            
            breakdown_data = []
            if st.session_state.forex_processed and st.session_state.forex_results:
                forex_summary = st.session_state.forex_results['summary']
                breakdown_data.append({
                    'Section': 'Riseworks (Forex)',
                    'Status': 'Completed ✅',
                    'Matched': forex_summary['matched'],
                    'Upload Only': forex_summary['upload_only'],
                    'Paste Only': forex_summary['paste_only'],
                    'Amount Differences': forex_summary['amount_differences']
                })
            else:
                breakdown_data.append({
                    'Section': 'Riseworks (Forex)',
                    'Status': 'Not Processed ❌',
                    'Matched': 0,
                    'Upload Only': 0,
                    'Paste Only': 0,
                    'Amount Differences': 0
                })
            
            
            if st.session_state.futures_processed and st.session_state.futures_results:
                futures_summary = st.session_state.futures_results['summary']
                breakdown_data.append({
                    'Section': 'Riseworks (Futures)',
                    'Status': 'Completed ✅',
                    'Matched': futures_summary['matched'],
                    'Upload Only': futures_summary['upload_only'],
                    'Paste Only': futures_summary['paste_only'],
                    'Amount Differences': futures_summary['amount_differences']
                })
            else:
                breakdown_data.append({
                    'Section': 'Riseworks (Futures)',
                    'Status': 'Not Processed ❌',
                    'Matched': 0,
                    'Upload Only': 0,
                    'Paste Only': 0,
                    'Amount Differences': 0
                })
            
            breakdown_df = pd.DataFrame(breakdown_data)
            st.dataframe(breakdown_df, use_container_width=True)
    
    else:
        st.info("Complete at least one reconciliation section to generate master summary")
    
    # Export Section
    if st.session_state.forex_results or st.session_state.futures_results:
        st.divider()
        st.header("📥 Export Results")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.session_state.forex_results:
                if st.button("📊 Export Forex Results"):
                    processor = ReconciliationProcessor()
                    excel_data = processor.export_results(st.session_state.forex_results, "Forex")
                    st.download_button(
                        label="💾 Download Forex Excel",
                        data=excel_data,
                        file_name=f"forex_reconciliation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
        
        with col2:
            if st.session_state.futures_results:
                if st.button("📊 Export Futures Results"):
                    processor = ReconciliationProcessor()
                    excel_data = processor.export_results(st.session_state.futures_results, "Futures")
                    st.download_button(
                        label="💾 Download Futures Excel",
                        data=excel_data,
                        file_name=f"futures_reconciliation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
        
        with col3:
            if st.session_state.forex_results or st.session_state.futures_results:
                if st.button("📊 Export Combined Results"):
                    processor = ReconciliationProcessor()
                    combined_data = processor.export_combined_results(
                        st.session_state.forex_results,
                        st.session_state.futures_results
                    )
                    st.download_button(
                        label="💾 Download Combined Excel",
                        data=combined_data,
                        file_name=f"combined_reconciliation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

# Reset functionality
st.divider()
col1, col2 = st.columns([1, 4])
with col1:
    if st.button("🔄 Reset All", type="secondary"):
        # Clear all session state
        for key in ['uploaded_data', 'forex_data', 'futures_data', 'forex_results', 
                    'futures_results', 'forex_processed', 'futures_processed']:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()

with col2:
    st.caption("Click to reset all data and start over")
