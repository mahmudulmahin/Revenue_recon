import streamlit as st
import pandas as pd
from datetime import datetime, date, time, timedelta
import io

# Performance optimization
@st.cache_data
def load_csv_file(file_content, parse_dates_cols):
    """Cache CSV loading for better performance"""
    return pd.read_csv(io.BytesIO(file_content), parse_dates=parse_dates_cols)

@st.cache_data
def load_excel_file(file_content, parse_dates_cols):
    """Cache Excel loading for better performance"""
    return pd.read_excel(io.BytesIO(file_content), parse_dates=parse_dates_cols)

st.set_page_config(page_title="Payment Gateway Comparator", layout="wide")
st.title("ZEN vs BridgerPay vs Coins Buy vs PayProcc Comparator")

# Create five tabs: ZEN, BridgerPay, Coins Buy, PayProcc, and Summary
tab_zen, tab_bp, tab_coins, tab_payprocc, tab_summary = st.tabs(["ZEN", "BridgerPay", "Coins Buy", "PayProcc", "Summary"])

# Futures filtering function - checks if "Futures" is in the Plan Type name
def is_futures_plan(plan_type):
    """Check if a plan type contains 'Futures' (case-insensitive)"""
    if pd.isna(plan_type):
        return False
    return "futures" in str(plan_type).lower()

# --- ZEN Tab ---
with tab_zen:
    st.header("ZEN vs Order-list")
    zen_file = st.file_uploader("Upload ZEN Settlement File", key="zen_file", type=["csv", "xlsx"])
    order_files_zen = st.file_uploader("Upload Order-list Files for ZEN (Multiple files allowed)", key="order_files_zen", type=["csv", "xlsx"], accept_multiple_files=True)

    if zen_file and order_files_zen:
        # Load ZEN with caching
        with st.spinner("Loading ZEN file..."):
            if zen_file.name.lower().endswith('.csv'):
                df_zen = load_csv_file(zen_file.read(), ["accepted_at"])
            else:
                df_zen = load_excel_file(zen_file.read(), ["accepted_at"])
            df_zen["accepted_at"] = df_zen["accepted_at"].dt.tz_localize(None)

        # Validate gateway columns
        if not (df_zen.get("Gateway", pd.Series()).eq("Zen Pay").all()):
            st.error("ZEN file must have Gateway='Zen Pay'")
            st.stop()

        # Load and merge multiple Order-list files for ZEN
        st.subheader("Step 2: Process ZEN Order List Files")
        st.info(f"Processing {len(order_files_zen)} Order List file(s)")
        
        # Load all Order List files
        order_list_dfs = []
        for i, order_file in enumerate(order_files_zen):
            if order_file.name.lower().endswith('.csv'):
                df_temp = pd.read_csv(order_file, parse_dates=["Updated At"])
            else:
                df_temp = pd.read_excel(order_file, parse_dates=["Updated At"])
            st.info(f"File {i+1} ({order_file.name}): {len(df_temp)} entries")
            order_list_dfs.append(df_temp)
        
        # Merge all Order List files
        df_ord = pd.concat(order_list_dfs, ignore_index=True)
        st.info(f"Combined Order List: {len(df_ord)} total entries before cleaning")
        
        if not all(g == "Zen Pay" for g in df_ord.get("Gateway", pd.Series()).unique()):
            st.error("Order-list must have Gateway='Zen Pay'")
            st.stop()
        df_ord = df_ord[df_ord.get("Gateway", "") == "Zen Pay"].copy()
        
        # Clean merged Order List: Remove duplicates and sort by datetime
        duplicates_ord = df_ord.duplicated(subset=["Transaction ID"], keep=False)
        if duplicates_ord.any():
            st.warning(f"Removed {duplicates_ord.sum()} duplicates from merged Order List")
            df_ord = df_ord.drop_duplicates(subset=["Transaction ID"], keep="first")
        
        # Sort by Updated At datetime
        df_ord = df_ord.sort_values("Updated At")
        
        st.success(f"Final merged Order List: {len(df_ord)} clean entries (sorted by datetime)")

        # Date range selection based on ZEN data
        min_date = df_zen['accepted_at'].dt.date.min()
        max_date = df_zen['accepted_at'].dt.date.max()
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start date", value=min_date)
        with col2:
            end_date = st.date_input("End date", value=max_date)

        # Define head/tail windows (GMT+2 offset for Order List)
        start_zen = datetime.combine(start_date, time(18, 0, 0))
        end_zen = datetime.combine(end_date, time(17, 59, 59))
        start_ord = datetime.combine(start_date, time(20, 0, 0))
        end_ord = datetime.combine(end_date, time(19, 59, 59))

        # Filter ZEN
        df_zen_filt = df_zen[
            (df_zen["accepted_at"] >= start_zen) &
            (df_zen["accepted_at"] <= end_zen) &
            (df_zen["payment_channel"].str.lower() != "card") &
            (df_zen["transaction_type"].str.lower() == "purchase")
        ].copy()

        # Filter non-USD and duplicates
        mask_currency_zen = df_zen_filt["transaction_currency"].str.upper() != "USD"
        if mask_currency_zen.any():
            st.warning(f"Excluded {mask_currency_zen.sum()} non-USD transactions")
            df_zen_filt = df_zen_filt.loc[~mask_currency_zen]

        initial_zen_count = len(df_zen_filt)
        duplicates_zen = df_zen_filt.duplicated(subset=["merchant_transaction_id"], keep=False)
        if duplicates_zen.any():
            st.warning(f"Removed {duplicates_zen.sum()} duplicates from {initial_zen_count} ZEN transactions")
            df_zen_filt = df_zen_filt.drop_duplicates(subset=["merchant_transaction_id"], keep="first")
        st.info(f"ZEN PSP: {len(df_zen_filt)} clean transactions")

        # Clean Order List
        initial_ord_count = len(df_ord)
        duplicates_ord = df_ord.duplicated(subset=["Transaction ID"], keep=False)
        if duplicates_ord.any():
            st.warning(f"Removed {duplicates_ord.sum()} duplicates from Order List")
            df_ord = df_ord.drop_duplicates(subset=["Transaction ID"], keep="first")
        st.info(f"Order List: {len(df_ord)} clean entries")

        # Filter Order-list and select needed columns
        df_ord_filt = df_ord[
            (df_ord["Updated At"] >= start_ord) &
            (df_ord["Updated At"] <= end_ord)
        ][["Transaction ID", "Plan Type", "Grand Total"]].copy()

        # Merge on transaction ID
        df_merged = df_zen_filt.merge(
            df_ord_filt,
            left_on="merchant_transaction_id",
            right_on="Transaction ID",
            how="inner"
        )

        # Check amount mismatches
        mask_amt = df_merged["transaction_amount"] != df_merged["Grand Total"]
        if mask_amt.any():
            st.warning(f"Found {mask_amt.sum()} amount mismatches in ZEN:")
            st.dataframe(df_merged.loc[mask_amt, [
                "merchant_transaction_id", "transaction_amount", "Grand Total", "transaction_currency"
            ]])
        else:
            st.success("No amount mismatches")
        
        # Handle unmatched PSP entries (add to CFD)
        unmatched_zen_psp = df_zen_filt[~df_zen_filt["merchant_transaction_id"].isin(df_merged["merchant_transaction_id"])]
        if len(unmatched_zen_psp) > 0:
            st.warning(f"Found {len(unmatched_zen_psp)} unmatched ZEN PSP entries - adding to CFD:")
            st.dataframe(unmatched_zen_psp[["merchant_transaction_id", "accepted_at", "transaction_amount", "transaction_currency"]])
            # Add unmatched entries to CFD
            unmatched_zen_psp = unmatched_zen_psp.copy()
            unmatched_zen_psp["Plan Type"] = "CFD (Unmatched PSP)"
            unmatched_zen_psp["Grand Total"] = unmatched_zen_psp["transaction_amount"]
            df_merged = pd.concat([df_merged, unmatched_zen_psp], ignore_index=True)

        st.info(f"Final total: {len(df_merged)} transactions included in export")

        # Split into Futures vs CFD
        df_futures = df_merged[df_merged["Plan Type"].apply(is_futures_plan)].copy()
        df_cfd = df_merged[~df_merged["Plan Type"].apply(is_futures_plan)].copy()

        # Sort by datetime before export
        df_futures = df_futures.sort_values("accepted_at")
        df_cfd = df_cfd.sort_values("accepted_at")

        # Revenue summary (GMT+6 shift on accepted_at)
        df_futures["Date"] = (df_futures["accepted_at"] + pd.Timedelta(hours=6)).dt.date
        df_cfd["Date"] = (df_cfd["accepted_at"] + pd.Timedelta(hours=6)).dt.date
        df_futures["Category"] = "Futures"
        df_cfd["Category"] = "CFD"
        df_summary = pd.concat([
            df_cfd[["Date", "Category", "transaction_amount"]],
            df_futures[["Date", "Category", "transaction_amount"]]
        ])
        df_summary = df_summary.groupby(["Date", "Category"], as_index=False).agg(Revenue=("transaction_amount", "sum")).sort_values("Date")

        st.subheader("Datewise Revenue Summary (GMT+6)")
        st.dataframe(df_summary)

        # Excel output for ZEN
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            cols = df_zen.columns.tolist()
            df_cfd[cols].to_excel(writer, sheet_name='CFD', index=False)
            df_futures[cols].to_excel(writer, sheet_name='Futures', index=False)
            df_summary.to_excel(writer, sheet_name='Revenue Summary', index=False)
            for sheet_name, df_out in [('CFD', df_cfd), ('Futures', df_futures), ('Revenue Summary', df_summary)]:
                ws = writer.sheets[sheet_name]
                for idx, col in enumerate(df_out.columns):
                    max_len = max(df_out[col].astype(str).map(len).max(), len(col)) + 2
                    ws.set_column(idx, idx, max_len)

        st.download_button(
            label="Download ZEN Comparison Report",
            data=output.getvalue(),
            file_name="zen_order_comparison.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
        # Store in session state for Summary tab
        st.session_state['zen_summary'] = df_summary.copy()
        st.session_state['zen_summary']['Gateway'] = 'ZEN'
    else:
        st.info("Please upload the ZEN Settlement file and at least one Order-list file for ZEN.")

# --- BridgerPay Tab ---
with tab_bp:
    st.header("BridgerPay vs Order-list")
    bp_file = st.file_uploader("Upload BridgerPay File", key="bp_file", type=["csv", "xlsx"])
    order_files_bp = st.file_uploader("Upload Order-list Files for BridgerPay (Multiple files allowed)", key="order_files_bp", type=["csv", "xlsx"], accept_multiple_files=True)

    if bp_file and order_files_bp:
        # Load BridgerPay
        if bp_file.name.lower().endswith('.csv'):
            df_bp = pd.read_csv(bp_file)
        else:
            df_bp = pd.read_excel(bp_file)
        
        # Check if processing_date column exists and parse it
        if "processing_date" in df_bp.columns:
            # Use format=None to auto-detect the format, and utc=True for timezone-aware parsing
            df_bp["processing_date"] = pd.to_datetime(df_bp["processing_date"], format='mixed', utc=True)
            # Remove timezone info to keep as naive datetime
            df_bp["processing_date"] = df_bp["processing_date"].dt.tz_localize(None)
        else:
            st.error(f"BridgerPay file must contain 'processing_date' column. Found columns: {list(df_bp.columns)}")
            st.stop()

        # Validate and filter BridgerPay
        if not (df_bp.get("Gateway", pd.Series()).eq("Bridger Pay").all()):
            st.error("BridgerPay file must have Gateway='Bridger Pay'")
            st.stop()

        df_bp = df_bp[
            (df_bp.get("status", "").str.lower() == "approved") &
            (df_bp.get("type", "").str.lower() == "payment") &
            (df_bp.get("currency", "").str.upper() == "USD")
        ]

        # Remove duplicates
        initial_count = len(df_bp)
        duplicates_bp = df_bp.duplicated(subset=["merchantOrderId"], keep=False)
        if duplicates_bp.any():
            st.warning(f"Removed {duplicates_bp.sum()} duplicates from {initial_count} BridgerPay transactions")
            df_bp = df_bp.drop_duplicates(subset=["merchantOrderId"], keep="first")
        st.info(f"BridgerPay PSP: {len(df_bp)} clean transactions")

        # Date range selection based on BridgerPay processing_date
        min_date_bp = df_bp['processing_date'].dt.date.min()
        max_date_bp = df_bp['processing_date'].dt.date.max()
        col1_bp, col2_bp = st.columns(2)
        with col1_bp:
            start_date_bp = st.date_input("Start date", value=min_date_bp, key="bp_start_date")
        with col2_bp:
            end_date_bp = st.date_input("End date", value=max_date_bp, key="bp_end_date")
        # Filter BridgerPay data for selected window (00:00 to 23:59:59)
        start_proc = datetime.combine(start_date_bp, time(0, 0, 0))
        end_proc   = datetime.combine(end_date_bp,   time(23, 59, 59))
        df_bp = df_bp[(df_bp['processing_date'] >= start_proc) & (df_bp['processing_date'] <= end_proc)].copy()
        
        # Define Order List window with GMT+2 offset
        start_ord_bp = datetime.combine(start_date_bp, time(2, 0, 0))
        end_ord_bp = datetime.combine(end_date_bp + timedelta(days=1), time(1, 59, 59))

        # Sort oldest→newest
        df_bp = df_bp.sort_values("processing_date")

        # Load and merge multiple Order-list files for BridgerPay
        st.subheader("Step 3: Process BridgerPay Order List Files")
        st.info(f"Processing {len(order_files_bp)} Order List file(s)")
        
        # Load all Order List files
        order_list_dfs = []
        for i, order_file in enumerate(order_files_bp):
            if order_file.name.lower().endswith('.csv'):
                df_temp = pd.read_csv(order_file, parse_dates=["Updated At"])
            else:
                df_temp = pd.read_excel(order_file, parse_dates=["Updated At"])
            st.info(f"File {i+1} ({order_file.name}): {len(df_temp)} entries")
            order_list_dfs.append(df_temp)
        
        # Merge all Order List files
        df_ord2 = pd.concat(order_list_dfs, ignore_index=True)
        st.info(f"Combined Order List: {len(df_ord2)} total entries before cleaning")
        
        if not all(g == "Bridger Pay" for g in df_ord2.get("Gateway", pd.Series()).unique()):
            st.error("Order-list must have Gateway='Bridger Pay'")
            st.stop()
        df_ord2 = df_ord2[df_ord2.get("Gateway", "") == "Bridger Pay"].copy()
        
        # Clean merged Order List: Remove duplicates and sort by datetime
        duplicates_ord = df_ord2.duplicated(subset=["Transaction ID"], keep=False)
        if duplicates_ord.any():
            st.warning(f"Removed {duplicates_ord.sum()} duplicates from merged Order List")
            df_ord2 = df_ord2.drop_duplicates(subset=["Transaction ID"], keep="first")
        
        # Sort by Updated At datetime
        df_ord2 = df_ord2.sort_values("Updated At")
        
        st.success(f"Final merged Order List: {len(df_ord2)} clean entries (sorted by datetime)")
        
        # Filter Order-list with GMT+2 offset window
        df_ord2 = df_ord2[(df_ord2["Updated At"] >= start_ord_bp) & (df_ord2["Updated At"] <= end_ord_bp)].copy()

        # Merge and amount reconciliation for BP
        df_ord2_sel = df_ord2[["Transaction ID", "Plan Type", "Grand Total"]].copy()
        df_merged2 = df_bp.merge(
            df_ord2_sel,
            left_on="merchantOrderId",
            right_on="Transaction ID",
            how="inner"
        )
        mask_amt2 = df_merged2["amount"] != df_merged2["Grand Total"]
        if mask_amt2.any():
            st.warning(f"Found {mask_amt2.sum()} amount mismatches in BridgerPay:")
            st.dataframe(df_merged2.loc[mask_amt2, [
                "merchantOrderId", "amount", "Grand Total", "currency"
            ]])
        else:
            st.success("No amount mismatches")
        
        # Handle unmatched PSP entries (add to CFD)
        unmatched_bp_psp = df_bp[~df_bp["merchantOrderId"].isin(df_merged2["merchantOrderId"])]
        if len(unmatched_bp_psp) > 0:
            st.warning(f"Found {len(unmatched_bp_psp)} unmatched BridgerPay PSP entries - adding to CFD:")
            st.dataframe(unmatched_bp_psp[["merchantOrderId", "processing_date", "amount", "currency"]])
            # Add unmatched entries to CFD
            unmatched_bp_psp = unmatched_bp_psp.copy()
            unmatched_bp_psp["Plan Type"] = "CFD (Unmatched PSP)"
            unmatched_bp_psp["Grand Total"] = unmatched_bp_psp["amount"]
            df_merged2 = pd.concat([df_merged2, unmatched_bp_psp], ignore_index=True)

        st.info(f"Final total: {len(df_merged2)} transactions included in export")

        # Split into Futures vs CFD
        df_futures2 = df_merged2[df_merged2["Plan Type"].apply(is_futures_plan)].copy()
        df_cfd2 = df_merged2[~df_merged2["Plan Type"].apply(is_futures_plan)].copy()

        # Sort by datetime before export
        df_futures2 = df_futures2.sort_values("processing_date")
        df_cfd2 = df_cfd2.sort_values("processing_date")

        # Revenue summary (GMT+6 shift on processing_date)
        df_futures2["Date"] = (df_futures2["processing_date"] + pd.Timedelta(hours=6)).dt.date
        df_cfd2["Date"] = (df_cfd2["processing_date"] + pd.Timedelta(hours=6)).dt.date
        df_futures2["Category"] = "Futures"
        df_cfd2["Category"] = "CFD"
        df_summary2 = pd.concat([
            df_cfd2[["Date", "Category", "amount"]],
            df_futures2[["Date", "Category", "amount"]]
        ])
        df_summary2 = df_summary2.groupby(["Date", "Category"], as_index=False).agg(Revenue=("amount", "sum")).sort_values("Date")

        st.subheader("Datewise Revenue Summary (GMT+6)")
        st.dataframe(df_summary2)

        # Excel output for BP
        output2 = io.BytesIO()
        with pd.ExcelWriter(output2, engine="xlsxwriter") as writer:
            cols_bp = df_bp.columns.tolist()
            df_cfd2[cols_bp].to_excel(writer, sheet_name='CFD', index=False)
            df_futures2[cols_bp].to_excel(writer, sheet_name='Futures', index=False)
            df_summary2.to_excel(writer, sheet_name='Revenue Summary', index=False)
            for sheet_name, df_out in [('CFD', df_cfd2), ('Futures', df_futures2), ('Revenue Summary', df_summary2)]:
                ws = writer.sheets[sheet_name]
                for idx, col in enumerate(df_out.columns):
                    max_len = max(df_out[col].astype(str).map(len).max(), len(col)) + 2
                    ws.set_column(idx, idx, max_len)

        st.download_button(
            label="Download BridgerPay Comparison Report",
            data=output2.getvalue(),
            file_name="bridgerpay_order_comparison.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
        # Store in session state for Summary tab
        st.session_state['bp_summary'] = df_summary2.copy()
        st.session_state['bp_summary']['Gateway'] = 'BridgerPay'
    else:
        st.info("Please upload the BridgerPay file and at least one Order-list file for BridgerPay.")

# --- Coins Buy Tab ---
with tab_coins:
    st.header("Coins Buy vs Order-list")
    coins_file = st.file_uploader("Upload Coins Buy File", key="coins_file", type=["csv", "xlsx"])
    order_files_coins = st.file_uploader("Upload Order-list Files for Coins Buy (Multiple files allowed)", key="order_files_coins", type=["csv", "xlsx"], accept_multiple_files=True)

    if coins_file and order_files_coins:
        # Load Coins Buy
        if coins_file.name.lower().endswith('.csv'):
            df_coins = pd.read_csv(coins_file, parse_dates=["Created"])
        else:
            df_coins = pd.read_excel(coins_file, parse_dates=["Created"])
        df_coins["Created"] = df_coins["Created"].dt.tz_localize(None)

        # Calculate actual amount as Amount * Rate
        if "Amount" not in df_coins.columns or "Rate" not in df_coins.columns:
            st.error("Coins Buy file must contain 'Amount' and 'Rate' columns for calculation.")
            st.stop()
        df_coins["calculated_amount"] = df_coins["Amount"] * df_coins["Rate"]
        
        # Filter and check duplicates
        mask_high_amount = df_coins["calculated_amount"] > 2500
        if mask_high_amount.any():
            st.warning(f"Excluded {mask_high_amount.sum()} transactions > 2500:")
            excluded_transactions = df_coins[mask_high_amount][["Tracking ID", "Created", "Amount", "Rate", "calculated_amount"]]
            st.dataframe(excluded_transactions)
            df_coins = df_coins[~mask_high_amount].copy()

        duplicates_coins = df_coins.duplicated(subset=["Tracking ID"], keep=False)
        if duplicates_coins.any():
            st.info(f"Keeping {duplicates_coins.sum()} PSP duplicates for revenue:")
            duplicate_coins_data = df_coins[duplicates_coins].sort_values("Tracking ID")
            st.dataframe(duplicate_coins_data[["Tracking ID", "Created", "Amount", "Rate", "calculated_amount"]])
        st.info(f"Coins Buy PSP: {len(df_coins)} total transactions")

        # Date range selection based on Coins Buy Created date
        min_date_coins = df_coins['Created'].dt.date.min()
        max_date_coins = df_coins['Created'].dt.date.max()
        col1_coins, col2_coins = st.columns(2)
        with col1_coins:
            start_date_coins = st.date_input("Start date", value=min_date_coins, key="coins_start_date")
        with col2_coins:
            end_date_coins = st.date_input("End date", value=max_date_coins, key="coins_end_date")
        
        # Filter Coins Buy data for selected window (00:00 to 23:59:59)
        start_created = datetime.combine(start_date_coins, time(0, 0, 0))
        end_created = datetime.combine(end_date_coins, time(23, 59, 59))
        df_coins = df_coins[(df_coins['Created'] >= start_created) & (df_coins['Created'] <= end_created)].copy()
        
        # Define Order List window with GMT+2 offset
        start_ord_coins = datetime.combine(start_date_coins, time(2, 0, 0))
        end_ord_coins = datetime.combine(end_date_coins + timedelta(days=1), time(1, 59, 59))

        # Sort oldest→newest
        df_coins = df_coins.sort_values("Created")

        # Load and merge multiple Order-list files for Coins Buy
        st.subheader("Step 3: Process Coins Buy Order List Files")
        st.info(f"Processing {len(order_files_coins)} Order List file(s)")
        
        # Load all Order List files
        order_list_dfs = []
        for i, order_file in enumerate(order_files_coins):
            if order_file.name.lower().endswith('.csv'):
                df_temp = pd.read_csv(order_file, parse_dates=["Updated At"])
            else:
                df_temp = pd.read_excel(order_file, parse_dates=["Updated At"])
            st.info(f"File {i+1} ({order_file.name}): {len(df_temp)} entries")
            order_list_dfs.append(df_temp)
        
        # Merge all Order List files
        df_ord3 = pd.concat(order_list_dfs, ignore_index=True)
        st.info(f"Combined Order List: {len(df_ord3)} total entries before cleaning")
        
        # Check if Tracking ID exists in both files
        if "Tracking ID" not in df_coins.columns:
            st.error("Coins Buy file must contain 'Tracking ID' column for matching.")
            st.stop()
        if "Tracking ID" not in df_ord3.columns:
            st.error("Order-list file must contain 'Tracking ID' column for matching.")
            st.stop()

        # Clean merged Order List: Remove None entries, duplicates, and sort by datetime
        initial_ord3_count = len(df_ord3)
        
        # Remove None/blank Tracking ID entries
        mask_none_tracking = df_ord3["Tracking ID"].isna() | (df_ord3["Tracking ID"].astype(str).str.strip().isin(["", "None", "none"]))
        if mask_none_tracking.any():
            st.warning(f"Removed {mask_none_tracking.sum()} 'None' entries from merged Order List")
            df_ord3 = df_ord3[~mask_none_tracking].copy()
        
        # Remove duplicates (keep first occurrence)
        duplicates_ord3 = df_ord3.duplicated(subset=["Tracking ID"], keep=False)
        if duplicates_ord3.any():
            st.warning(f"Removed {duplicates_ord3.sum()} duplicates from merged Order List")
            df_ord3 = df_ord3.drop_duplicates(subset=["Tracking ID"], keep="first")
        
        # Sort by Updated At datetime
        df_ord3 = df_ord3.sort_values("Updated At")
        
        st.success(f"Final merged Order List: {len(df_ord3)} clean entries (sorted by datetime)")

        # Filter Order-list with GMT+2 offset window
        df_ord3 = df_ord3[(df_ord3["Updated At"] >= start_ord_coins) & (df_ord3["Updated At"] <= end_ord_coins)].copy()

        # Handle blank Tracking ID and match
        mask_blank_tracking = df_coins["Tracking ID"].isna() | (df_coins["Tracking ID"].astype(str).str.strip() == "")
        df_blank_tracking = df_coins[mask_blank_tracking].copy()
        df_coins_with_tracking = df_coins[~mask_blank_tracking].copy()
        
        if len(df_blank_tracking) > 0:
            st.warning(f"Assigned {len(df_blank_tracking)} blank Tracking ID entries to CFD")
            df_blank_tracking["Plan Type"] = "CFD (Blank Tracking ID)"
            df_blank_tracking["Grand Total"] = df_blank_tracking["calculated_amount"]

        # Match with Order List
        df_ord3_sel = df_ord3[["Tracking ID", "Plan Type", "Grand Total"]].copy()
        df_merged3 = df_coins_with_tracking.merge(df_ord3_sel, on="Tracking ID", how="inner")
        
        if len(df_blank_tracking) > 0:
            df_merged3 = pd.concat([df_merged3, df_blank_tracking], ignore_index=True)
        
        initial_matched_count = len(df_merged3)
        st.success(f"Initial matches: {initial_matched_count - len(df_blank_tracking)} with Order List + {len(df_blank_tracking)} CFD (blank)")
        
        # Handle unmatched PSP entries (add to CFD)
        unmatched_coins_psp = df_coins_with_tracking[~df_coins_with_tracking["Tracking ID"].isin(df_ord3["Tracking ID"])]
        if len(unmatched_coins_psp) > 0:
            st.warning(f"Found {len(unmatched_coins_psp)} unmatched Coins Buy PSP entries - adding to CFD:")
            st.dataframe(unmatched_coins_psp[["Tracking ID", "Created", "calculated_amount"]])
            # Add unmatched entries to CFD
            unmatched_coins_psp = unmatched_coins_psp.copy()
            unmatched_coins_psp["Plan Type"] = "CFD (Unmatched PSP)"
            unmatched_coins_psp["Grand Total"] = unmatched_coins_psp["calculated_amount"]
            df_merged3 = pd.concat([df_merged3, unmatched_coins_psp], ignore_index=True)

        st.info(f"Final total: {len(df_merged3)} transactions included in export")

        # Sort all entries by Created datetime
        df_merged3 = df_merged3.sort_values("Created")

        # Amount reconciliation (no display needed for Coins Buy)

        # Split into Futures vs CFD
        df_futures3 = df_merged3[df_merged3["Plan Type"].apply(is_futures_plan)].copy()
        df_cfd3 = df_merged3[~df_merged3["Plan Type"].apply(is_futures_plan)].copy()

        # Revenue summary (GMT+6 shift on Created)
        df_futures3["Date"] = (df_futures3["Created"] + pd.Timedelta(hours=6)).dt.date
        df_cfd3["Date"] = (df_cfd3["Created"] + pd.Timedelta(hours=6)).dt.date
        df_futures3["Category"] = "Futures"
        df_cfd3["Category"] = "CFD"
        df_summary3 = pd.concat([
            df_cfd3[["Date", "Category", "calculated_amount"]],
            df_futures3[["Date", "Category", "calculated_amount"]]
        ])
        df_summary3 = df_summary3.groupby(["Date", "Category"], as_index=False).agg(Revenue=("calculated_amount", "sum"))

        st.subheader("Datewise Revenue Summary (GMT+6)")
        st.dataframe(df_summary3)

        # Excel output for Coins Buy
        output3 = io.BytesIO()
        with pd.ExcelWriter(output3, engine="xlsxwriter") as writer:
            cols_coins = df_coins.columns.tolist()
            df_cfd3[cols_coins].to_excel(writer, sheet_name='CFD', index=False)
            df_futures3[cols_coins].to_excel(writer, sheet_name='Futures', index=False)
            df_summary3.to_excel(writer, sheet_name='Revenue Summary', index=False)
            for sheet_name, df_out in [('CFD', df_cfd3), ('Futures', df_futures3), ('Revenue Summary', df_summary3)]:
                ws = writer.sheets[sheet_name]
                for idx, col in enumerate(df_out.columns):
                    max_len = max(df_out[col].astype(str).map(len).max(), len(col)) + 2
                    ws.set_column(idx, idx, max_len)

        st.download_button(
            label="Download Coins Buy Comparison Report",
            data=output3.getvalue(),
            file_name="coinsbuy_order_comparison.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
        # Store in session state for Summary tab
        st.session_state['coins_summary'] = df_summary3.copy()
        st.session_state['coins_summary']['Gateway'] = 'Coins Buy'
    else:
        st.info("Please upload the Coins Buy file and at least one Order-list file for Coins Buy.")

# --- PayProcc Tab ---
with tab_payprocc:
    st.header("PayProcc Revenue Report")
    payprocc_file = st.file_uploader("Upload PayProcc File", key="payprocc_file", type=["csv", "xlsx", "txt"])

    if payprocc_file:
        # Load PayProcc file (simple like other tabs)
        st.subheader("Step 1: Load PayProcc File")
        if payprocc_file.name.lower().endswith('.csv') or payprocc_file.name.lower().endswith('.txt'):
            df_payprocc = pd.read_csv(payprocc_file)
        else:
            df_payprocc = pd.read_excel(payprocc_file)
        
        # Parse Transaction Date
        if "Transaction Date" in df_payprocc.columns:
            df_payprocc["Transaction Date"] = pd.to_datetime(df_payprocc["Transaction Date"])
        else:
            st.error(f"PayProcc file must contain 'Transaction Date' column. Found columns: {list(df_payprocc.columns)}")
            st.stop()
        
        st.success(f"Loaded {len(df_payprocc)} PayProcc transactions")
        
        # Validate required columns
        required_cols = ["Payment Public ID", "Amount", "Exchange Rate", "Description", "Type", "Status"]
        missing_cols = [col for col in required_cols if col not in df_payprocc.columns]
        if missing_cols:
            st.error(f"PayProcc file must contain these columns: {', '.join(missing_cols)}. Found columns: {list(df_payprocc.columns)}")
            st.stop()
        
        # Filter for Type = "sale" and Status = "success"
        initial_count_pp = len(df_payprocc)
        df_payprocc = df_payprocc[
            (df_payprocc["Type"].str.lower() == "sale") & 
            (df_payprocc["Status"].str.lower() == "success")
        ].copy()
        st.info(f"Filtered {initial_count_pp} → {len(df_payprocc)} transactions (Type=sale, Status=success)")
        
        # Remove duplicates by Payment Public ID
        st.subheader("Step 2: Remove Duplicates")
        initial_count_pp = len(df_payprocc)
        duplicates_pp = df_payprocc.duplicated(subset=["Payment Public ID"], keep=False)
        if duplicates_pp.any():
            st.warning(f"Found {duplicates_pp.sum()} duplicate transactions (removed)")
            df_payprocc = df_payprocc.drop_duplicates(subset=["Payment Public ID"], keep="first")
        st.info(f"PayProcc: {len(df_payprocc)} clean transactions after duplicate removal")
        
        # Date range selection
        st.subheader("Step 3: Select Date Range")
        min_date_pp = df_payprocc['Transaction Date'].dt.date.min()
        max_date_pp = df_payprocc['Transaction Date'].dt.date.max()
        col1_pp, col2_pp = st.columns(2)
        with col1_pp:
            start_date_pp = st.date_input("Start date", value=min_date_pp, key="pp_start_date")
        with col2_pp:
            end_date_pp = st.date_input("End date", value=max_date_pp, key="pp_end_date")
        
        # Filter by date range (already in GMT+6, no offset needed)
        start_dt_pp = datetime.combine(start_date_pp, time(0, 0, 0))
        end_dt_pp = datetime.combine(end_date_pp, time(23, 59, 59))
        df_payprocc = df_payprocc[(df_payprocc['Transaction Date'] >= start_dt_pp) & 
                                   (df_payprocc['Transaction Date'] <= end_dt_pp)].copy()
        
        st.info(f"Filtered to {len(df_payprocc)} transactions in selected date range")
        
        # Calculate final amount
        st.subheader("Step 4: Calculate Final Amount")
        # If Exchange Rate is empty, use Amount directly (already in USD)
        # If Exchange Rate has value, calculate Amount / Exchange Rate
        df_payprocc["Final Amount"] = df_payprocc.apply(
            lambda row: row["Amount"] if pd.isna(row["Exchange Rate"]) or row["Exchange Rate"] == 0 
            else row["Amount"] / row["Exchange Rate"], 
            axis=1
        )
        st.success(f"Calculated Final Amount (USD conversion applied)")
        
        # Split into Futures and CFD based on Description
        st.subheader("Step 5: Split by Category")
        
        # Check if Description contains "futures" (case-insensitive)
        df_payprocc["is_futures"] = df_payprocc["Description"].apply(
            lambda x: "futures" in str(x).lower() if pd.notna(x) else False
        )
        
        df_futures_pp = df_payprocc[df_payprocc["is_futures"]].copy()
        df_cfd_pp = df_payprocc[~df_payprocc["is_futures"]].copy()
        
        st.info(f"Futures: {len(df_futures_pp)} transactions | CFD: {len(df_cfd_pp)} transactions")
        
        # Sort by Transaction Date (ascending order)
        df_futures_pp = df_futures_pp.sort_values("Transaction Date")
        df_cfd_pp = df_cfd_pp.sort_values("Transaction Date")
        
        # Revenue summary (already in GMT+6)
        st.subheader("Step 6: Revenue Summary (GMT+6)")
        df_futures_pp["Date"] = df_futures_pp["Transaction Date"].dt.date
        df_cfd_pp["Date"] = df_cfd_pp["Transaction Date"].dt.date
        df_futures_pp["Category"] = "Futures"
        df_cfd_pp["Category"] = "CFD"
        
        df_summary_pp = pd.concat([
            df_cfd_pp[["Date", "Category", "Final Amount"]],
            df_futures_pp[["Date", "Category", "Final Amount"]]
        ])
        df_summary_pp = df_summary_pp.groupby(["Date", "Category"], as_index=False).agg(Revenue=("Final Amount", "sum"))
        
        st.subheader("Datewise Revenue Summary")
        st.dataframe(df_summary_pp)
        
        # Excel output for PayProcc
        st.subheader("Step 7: Download Report")
        output_pp = io.BytesIO()
        with pd.ExcelWriter(output_pp, engine="xlsxwriter") as writer:
            # Write sheets
            df_cfd_pp.to_excel(writer, sheet_name='CFD', index=False)
            df_futures_pp.to_excel(writer, sheet_name='Futures', index=False)
            df_summary_pp.to_excel(writer, sheet_name='Revenue Summary', index=False)
            
            # Auto-adjust column widths
            for sheet_name, df_out in [('CFD', df_cfd_pp), ('Futures', df_futures_pp), ('Revenue Summary', df_summary_pp)]:
                ws = writer.sheets[sheet_name]
                for idx, col in enumerate(df_out.columns):
                    max_len = max(df_out[col].astype(str).map(len).max(), len(col)) + 2
                    ws.set_column(idx, idx, max_len)

        st.download_button(
            label="Download PayProcc Revenue Report",
            data=output_pp.getvalue(),
            file_name="payprocc_revenue_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
        # Store in session state for Summary tab
        st.session_state['payprocc_summary'] = df_summary_pp.copy()
        st.session_state['payprocc_summary']['Gateway'] = 'PayProcc'
    else:
        st.info("Please upload the PayProcc file to generate revenue report.")

# --- Summary Tab ---
with tab_summary:
    st.header("Combined Gateway Revenue Summary")
    st.write("This tab shows the combined revenue summary from all four gateways.")
    
    # Collect summaries from session state
    summaries = []
    gateways_processed = []
    
    if 'zen_summary' in st.session_state:
        summaries.append(st.session_state['zen_summary'])
        gateways_processed.append("ZEN")
    
    if 'bp_summary' in st.session_state:
        summaries.append(st.session_state['bp_summary'])
        gateways_processed.append("BridgerPay")
    
    if 'coins_summary' in st.session_state:
        summaries.append(st.session_state['coins_summary'])
        gateways_processed.append("Coins Buy")
    
    if 'payprocc_summary' in st.session_state:
        summaries.append(st.session_state['payprocc_summary'])
        gateways_processed.append("PayProcc")
    
    if summaries:
        # Combine all summaries
        df_combined = pd.concat(summaries, ignore_index=True)
        
        # Group by Date, Category, and Gateway
        df_combined_grouped = df_combined.groupby(["Date", "Category", "Gateway"], as_index=False).agg(Revenue=("Revenue", "sum"))
        
        st.success(f"Showing data from: {', '.join(gateways_processed)}")
        
        # Show combined summary
        st.subheader("Combined Revenue by Date and Category")
        
        # Pivot table for better visualization
        df_pivot = df_combined_grouped.pivot_table(
            index='Date', 
            columns=['Category', 'Gateway'], 
            values='Revenue', 
            fill_value=0,
            aggfunc='sum'
        ).reset_index()
        
        st.dataframe(df_pivot, use_container_width=True)
        
        # Total by Gateway
        st.subheader("Total Revenue by Gateway")
        df_gateway_totals = df_combined_grouped.groupby("Gateway", as_index=False).agg(Total=("Revenue", "sum"))
        st.dataframe(df_gateway_totals)
        
        # Total by Category
        st.subheader("Total Revenue by Category")
        df_category_totals = df_combined_grouped.groupby("Category", as_index=False).agg(Total=("Revenue", "sum"))
        st.dataframe(df_category_totals)
        
        # Grand Total
        grand_total = df_combined_grouped["Revenue"].sum()
        st.metric("Grand Total Revenue", f"${grand_total:,.2f}")
        
        # Download combined summary
        output_summary = io.BytesIO()
        with pd.ExcelWriter(output_summary, engine="xlsxwriter") as writer:
            df_combined_grouped.to_excel(writer, sheet_name='Combined Summary', index=False)
            df_gateway_totals.to_excel(writer, sheet_name='Gateway Totals', index=False)
            df_category_totals.to_excel(writer, sheet_name='Category Totals', index=False)
        
        st.download_button(
            label="Download Combined Summary Report",
            data=output_summary.getvalue(),
            file_name="combined_gateway_summary.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("Please process at least one gateway (ZEN, BridgerPay, Coins Buy, or PayProcc) to see the combined summary.")
        st.write("**Instructions:**")
        st.write("1. Go to any gateway tab (ZEN, BridgerPay, Coins Buy, or PayProcc)")
        st.write("2. Upload the required files and generate the report")
        st.write("3. Return to this Summary tab to see the combined data")

